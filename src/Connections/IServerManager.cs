using System;
using System.Collections.Generic;

namespace FluentCassandra.Connections
{
	public interface IServerManager :IDisposable
	{

		void ErrorOccurred(Server server, Exception exc = null);
	    Server GetServer();
	}
}
