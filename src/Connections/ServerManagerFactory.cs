using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Connections
{
    public interface IServerManagerFactory {
        IServerManager Get(ServerCollection servers);
    }

    public class ServerManagerFactory : IServerManagerFactory {
		private readonly object _lock = new object();
		private volatile IDictionary<int, IServerManager> _managers = new Dictionary<int, IServerManager>();

        public IServerManager Get(ServerCollection servers)
		{
			lock(_lock)
			{
				IServerManager manager;

				if(!_managers.TryGetValue(servers.GetHashCode(), out manager))
				{
					if(servers.Count() == 1) {
					    manager = new SingleServerManager(servers.First(), servers.ServerPollingInterval);
					} else {
					    manager = new RoundRobinServerManager(servers);
					}
                    _managers.Add(servers.GetHashCode(), manager);
				}

				return manager;
			}
		}
	}
}