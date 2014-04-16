using System;
using System.Net.Sockets;

namespace FluentCassandra.Connections
{
	public class NormalConnectionProvider : IConnectionProvider
	{
	    private readonly IServerManager _serverManager;

	    /// <summary>
		/// 
		/// </summary>
		/// <param name="cluster"></param>
		public NormalConnectionProvider(IServerManager serverManager, Cluster cluster)
		{
		    _serverManager = serverManager;
		    if (cluster.Count > 1 && cluster.ConnectionTimeout == TimeSpan.Zero)
				throw new CassandraException("You must specify a timeout when using multiple servers.");

			ConnectionTimeout = cluster.ConnectionTimeout;
		    ConnectionType = cluster.ConnectionType;
		    BufferSize = cluster.BufferSize;
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
		/// <returns></returns>
		public IConnection Open()
		{
			IConnection conn = null;
		    Server server = null;
			while ((server = _serverManager.GetServer()) != null)
			{
				try {
                    conn = new Connection(server, ConnectionType, BufferSize);
					conn.Open();
					break;
				}
				catch (SocketException exc)
				{
					_serverManager.ErrorOccurred(conn.Server, exc);
					Close(conn);
					conn = null;
				}
			}

			if (conn == null)
				throw new CassandraException("No connection could be made because all servers have failed.");

			return conn;
		}

	    public void ErrorOccurred(IConnection connection, Exception exc = null)
		{
			try {
                Close(connection);
            } catch { }
            _serverManager.ErrorOccurred(connection.Server, exc);
		}

	    public void Close(IConnection connection) {
            if(connection.IsOpen)
                connection.Close();
        }

	    public void Dispose() {}
	}
}