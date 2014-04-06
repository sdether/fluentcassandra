using System;

namespace FluentCassandra.Connections
{
	public abstract class ConnectionProvider : IConnectionProvider
	{
	    protected readonly IServerManager _serverManager;

	    /// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		protected ConnectionProvider(IServerManager serverManager) {
		    _serverManager = serverManager;
		}

	    public abstract IConnection Open();

	    public abstract void ErrorOccurred(IConnection connection, Exception exc = null);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connection"></param>
		/// <returns></returns>
		public virtual void Close(IConnection connection)
		{
			if (connection.IsOpen)
				connection.Close();
		}

	    public virtual void Dispose() {}
	}
}
