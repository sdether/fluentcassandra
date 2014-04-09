using System;
using System.Net.Sockets;

namespace FluentCassandra.Connections
{
	public class NormalConnectionProvider : ConnectionProvider
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="cluster"></param>
		public NormalConnectionProvider(IServerManager serverManager, Cluster cluster)
            : base(serverManager)
		{
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
		public override IConnection Open()
		{
			IConnection conn = null;
		    Server server = null;
			while ((server = _serverManager.GetServer()) != null)
			{
				try {
				    conn = CreateConnection(server);
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

	    protected virtual IConnection CreateConnection(Server server) {
	        return new Connection(server, ConnectionType, BufferSize);
	    }

	    public override void ErrorOccurred(IConnection connection, Exception exc = null)
		{
			try { Close(connection); } catch { }
            _serverManager.ErrorOccurred(connection.Server, exc);
		}
	}
}