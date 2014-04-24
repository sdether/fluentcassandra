using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;

namespace FluentCassandra.Connections {
    public class PooledConnectionProvider : IConnectionProvider {
        private readonly object _lock = new object();
        private readonly IServerManager _serverManager;
        private readonly HashSet<IConnection> _usedConnections = new HashSet<IConnection>();
        private readonly Timer _maintenanceTimer;
        private readonly Func<Server, ConnectionType, int, IConnection> _connectionFactory;
        private readonly int _minPoolSize;
        private readonly int _maxPoolSize;
        private readonly ConnectionType _connectionType;
        private readonly int _bufferSize;
        private readonly TimeSpan _connectionLifetime;
        private Queue<IConnection> _freeConnections = new Queue<IConnection>();
        private bool _isDisposed;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverManager"></param>
        /// <param name="cluster"></param>
        /// <param name="connectionFactory">Optional custom connection factory (such as for testing)</param>
        public PooledConnectionProvider(IServerManager serverManager, Cluster cluster, Func<Server, ConnectionType, int, IConnection> connectionFactory = null) {
            _connectionFactory = connectionFactory ?? ((server, connectionType, bufferSize) => new Connection(server, connectionType, bufferSize));
            _serverManager = serverManager;
            _minPoolSize = cluster.MinPoolSize;
            _maxPoolSize = cluster.MaxPoolSize;
            _connectionType = cluster.ConnectionType;
            _connectionLifetime = cluster.ConnectionLifetime;
            _bufferSize = cluster.BufferSize;
            _maintenanceTimer = new Timer(o => CheckFreeConnectionsAlive(), null, 30000L, 30000L);
        }

        public IConnection Open() {
            if(_isDisposed) {
                throw new ObjectDisposedException("PooledConnectionProvider", "PooledConnectionProvider is already disposed");
            }

            IConnection conn = null;
            while(conn == null) {
                lock(_lock) {
                    var poolSize = _freeConnections.Count + _usedConnections.Count;
                    if(poolSize >= _minPoolSize && _freeConnections.Count > 0) {
                        conn = _freeConnections.Dequeue();
                        if(!conn.IsOpen) {
                            conn.Dispose();
                            continue;
                        }
                        break;
                    }
                    if(poolSize >= _maxPoolSize) {
                        if(!Monitor.Wait(_lock, TimeSpan.FromSeconds(30)))
                            throw new TimeoutException("No connection could be made, timed out trying to acquirer a connection from the connection pool.");

                        continue;
                    }
                    var server = _serverManager.GetServer();
                    if(server == null) {
                        throw new CassandraException("No connection could be made because all servers have failed.");
                    }
                    try {
                        conn = _connectionFactory(server, _connectionType, _bufferSize);
                        conn.Open();
                        break;
                    } catch(SocketException exc) {

                        // the only time we fail the server related to a connection is if the opening of the connection failed.
                        // An already open connection failing is not indicative of a server failure and it is better to
                        // assume it was only a per connection failure.
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
                try {
                    Close(connection);
                } catch { }
                _usedConnections.Remove(connection);
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
                else
                    connection.Close();
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
            if(_connectionLifetime > TimeSpan.Zero && connection.Created.Add(_connectionLifetime) < DateTime.UtcNow)
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
                if(!_freeConnections.Any()) {
                    return;
                }
                var freeConnections = _freeConnections.ToArray();
                var liveConnections = new List<IConnection>();
                foreach(var free in freeConnections) {
                    if(IsAlive(free)) {
                        liveConnections.Add(free);
                        continue;
                    }
                    if(free.IsOpen)
                        free.Close();
                }
                if(freeConnections.Length == liveConnections.Count) {
                    return;
                }
                _freeConnections = new Queue<IConnection>(liveConnections);
            }
        }

        public void Dispose() {
            if(_isDisposed) {
                return;
            }
            lock(_lock) {
                if(_isDisposed) {
                    return;
                }
                _isDisposed = true;
                foreach(var conn in _usedConnections) {
                    try {
                        conn.Close();
                    } catch { }
                }
                _usedConnections.Clear();
                foreach(var conn in _freeConnections) {
                    try {
                        conn.Close();
                    } catch { }
                }
                _freeConnections.Clear();
            }
            _maintenanceTimer.Dispose();
        }
    }
}