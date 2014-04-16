using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;

namespace FluentCassandra.Connections {
    public class PooledConnectionProvider : IConnectionProvider {
        private readonly object _lock = new object();
        private readonly IServerManager _serverManager;
        private readonly Queue<IConnection> _freeConnections = new Queue<IConnection>();
        private readonly HashSet<IConnection> _usedConnections = new HashSet<IConnection>();
        private readonly Timer _maintenanceTimer;
        private readonly Func<Server, ConnectionType, int, IConnection> _connectionFactory;
        private bool _isDisposed;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverManager"></param>
        /// <param name="cluster"></param>
        /// <param name="connectionFactory">Optional custom connection factory (such as for testing)</param>
        public PooledConnectionProvider(IServerManager serverManager, Cluster cluster, Func<Server,ConnectionType,int,IConnection> connectionFactory = null) {
            _connectionFactory = connectionFactory ?? ((server, connectionType, bufferSize) => new Connection(server,connectionType,bufferSize));
            _serverManager = serverManager;
            MinPoolSize = cluster.MinPoolSize;
            MaxPoolSize = cluster.MaxPoolSize;
            ConnectionTimeout = cluster.ConnectionTimeout;
            ConnectionType = cluster.ConnectionType;
            BufferSize = cluster.BufferSize;
            _maintenanceTimer = new Timer(o => CheckFreeConnectionsAlive(), null, 30000L, 30000L);
        }

        /// <summary>
        /// 
        /// </summary>
        public TimeSpan ConnectionTimeout { get; private set; }
        public ConnectionType ConnectionType { get; private set; }
        public int BufferSize { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public int MinPoolSize { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public int MaxPoolSize { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public TimeSpan ConnectionLifetime { get; private set; }

        public IConnection Open() {
            if(_isDisposed) {
                throw new ObjectDisposedException("PooledConnectionProvider", "PooledConnectionProvider is already disposed");
            }

            IConnection conn = null;
            while(conn == null) {
                lock(_lock) {
                    var poolSize = _freeConnections.Count + _usedConnections.Count;
                    if(poolSize >= MinPoolSize && _freeConnections.Count > 0) {
                        conn = _freeConnections.Dequeue();
                        if(!conn.IsOpen) {
                            conn.Dispose();
                            continue;
                        }
                        _usedConnections.Add(conn);
                        break;
                    }
                    if(poolSize >= MaxPoolSize) {
                        if(!Monitor.Wait(_lock, TimeSpan.FromSeconds(30)))
                            throw new TimeoutException("No connection could be made, timed out trying to acquirer a connection from the connection pool.");

                        continue;
                    }
                    var server = _serverManager.GetServer();
                    if(server == null) {
                        throw new CassandraException("No connection could be made because all servers have failed.");
                    }
                    try {
                        conn = _connectionFactory(server, ConnectionType, BufferSize);
                        conn.Open();
                        break;
                    } catch(SocketException exc) {
                        _serverManager.ErrorOccurred(conn.Server, exc);
                        Close(conn);
                        conn = null;
                    }
                }
            }
            _usedConnections.Add(conn);
            return conn;
        }

        public void ErrorOccurred(IConnection connection, Exception exc = null) {
            if(_isDisposed) {
                return;
            }
            lock(_lock) {
                _usedConnections.RemoveWhere(x => x.Server.Equals(connection.Server));
                _serverManager.ErrorOccurred(connection.Server, exc);

                var currentFreeConnections = _freeConnections.ToArray();
                _freeConnections.Clear();

                foreach(var conn in currentFreeConnections)
                    if(!conn.Server.Equals(connection.Server))
                        _freeConnections.Enqueue(conn);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public void Close(IConnection connection) {
            if(_isDisposed) {
                return;
            }
            lock(_lock) {
                _usedConnections.Remove(connection);

                if(IsAlive(connection))
                    _freeConnections.Enqueue(connection);
            }
        }

        /// <summary>
        /// Determines whether the connection is alive.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <returns>True if alive; otherwise false.</returns>
        private bool IsAlive(IConnection connection) {
            if(_isDisposed) {
                return false;
            }
            if(ConnectionLifetime > TimeSpan.Zero && connection.Created.Add(ConnectionLifetime) < DateTime.UtcNow)
                return false;

            return connection.IsOpen;
        }

        /// <summary>
        /// The check free connections alive.
        /// </summary>
        private void CheckFreeConnectionsAlive() {
            if(_isDisposed) {
                return;
            }
            lock(_lock) {
                var freeConnections = _freeConnections.ToArray();
                _freeConnections.Clear();

                foreach(var free in freeConnections) {
                    if(IsAlive(free))
                        _freeConnections.Enqueue(free);
                    else if(free.IsOpen)
                        free.Close();
                }
            }
        }

        public void Dispose() {
            if(_isDisposed) {
                return;
            }
            _isDisposed = true;
            lock(_lock) {
                foreach(var conn in _usedConnections) {
                    conn.Close();
                }
                _usedConnections.Clear();
                foreach(var conn in _freeConnections) {
                    conn.Close();
                }
                _freeConnections.Clear();
            }
            _maintenanceTimer.Dispose();
        }
    }
}