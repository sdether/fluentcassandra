using System;
using System.Net.Sockets;

namespace FluentCassandra.Connections {
    public class NormalConnectionProvider : IConnectionProvider {
        private readonly IServerManager _serverManager;
        private readonly ConnectionType _connectionType;
        private readonly int _bufferSize;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cluster"></param>
        public NormalConnectionProvider(IServerManager serverManager, Cluster cluster) {
            _serverManager = serverManager;
            if(cluster.Count > 1 && cluster.ConnectionTimeout == TimeSpan.Zero)
                throw new CassandraException("You must specify a timeout when using multiple servers.");

            _connectionType = cluster.ConnectionType;
            _bufferSize = cluster.BufferSize;
        }

        public IConnection Open() {
            IConnection conn = null;
            Server server;
            while((server = _serverManager.GetServer()) != null) {
                try {
                    conn = new Connection(server, _connectionType, _bufferSize);
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

            if(conn == null)
                throw new CassandraException("No connection could be made because all servers have failed.");

            return conn;
        }

        public void ErrorOccurred(IConnection connection, Exception exc = null) {
            try {
                Close(connection);
            } catch { }
        }

        public void Close(IConnection connection) {
            if(connection.IsOpen)
                connection.Close();
        }

        public void Dispose() { }
    }
}