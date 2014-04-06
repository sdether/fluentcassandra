using System;

namespace FluentCassandra.Connections
{
	public interface IConnectionProvider : IDisposable {
        
		IConnection Open();

		void ErrorOccurred(IConnection connection, Exception exc = null);

		void Close(IConnection connection);
	}
}
