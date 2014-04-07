using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FluentCassandra.Connections {
    public interface IConnectionProviderRepository {
        IConnectionProvider Get(IConnectionBuilder builder);
    }

    public class AutoDisposingConnectionProviderRepository : IConnectionProviderRepository {

        private class ReferenceCountingProviderWrapper : IConnectionProvider {
            private readonly IConnectionProvider _connectionProvider;
            private readonly IServerManager _serverManager;
            private int _references;

            public ReferenceCountingProviderWrapper(IConnectionProvider connectionProvider, IServerManager serverManager) {
                _connectionProvider = connectionProvider;
                _serverManager = serverManager;
                _references = 1;
            }

            public bool Active { get { return _references > 0; } }
            public IServerManager ServerManager { get { return _serverManager; } }

            public void IncrementReferences() {
                Interlocked.Increment(ref _references);
            }


            public void Dispose() {
                Interlocked.Decrement(ref _references);
            }

            public IConnection Open() {
                return _connectionProvider.Open();
            }

            public void ErrorOccurred(IConnection connection, Exception exc = null) {
                _connectionProvider.ErrorOccurred(connection, exc);
            }

            public void Close(IConnection connection) {
                _connectionProvider.Close(connection);
            }

            public void DisposeInternals() {
                _serverManager.Dispose();
                _connectionProvider.Dispose();
            }
        }

        public static readonly IConnectionProviderRepository Instance = new AutoDisposingConnectionProviderRepository();

        private readonly object _lock = new object();
        private readonly Dictionary<int, ReferenceCountingProviderWrapper> _poolingProviders = new Dictionary<int, ReferenceCountingProviderWrapper>();
        private readonly Dictionary<int, ReferenceCountingProviderWrapper> _singleProviders = new Dictionary<int, ReferenceCountingProviderWrapper>();

        private AutoDisposingConnectionProviderRepository() {
            new Timer(Cleanup, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        }

        public IConnectionProvider Get(IConnectionBuilder builder) {
            lock(_lock) {
                ReferenceCountingProviderWrapper provider;
                var servers = builder.GetServerCollection();
                var hashcode = servers.GetHashCode();
                var serverManager = CreateServerManager(servers);
                if(builder.Pooling) {
                    if(_poolingProviders.TryGetValue(hashcode, out provider) && provider.Active) {
                        provider.IncrementReferences();
                    } else {
                        provider = new ReferenceCountingProviderWrapper(new PooledConnectionProvider(serverManager, builder), serverManager);
                        _poolingProviders[hashcode] = provider;
                    }
                } else {
                    if(_singleProviders.TryGetValue(hashcode, out provider) && provider.Active) {
                        provider.IncrementReferences();
                    } else {
                        provider = new ReferenceCountingProviderWrapper(new PooledConnectionProvider(serverManager, builder), serverManager);
                        _singleProviders[hashcode] = provider;
                    }
                }
                return provider;
            }
        }
        
        private IServerManager CreateServerManager(ServerCollection servers) {
            return servers.Count() == 1
                ? (IServerManager)new SingleServerManager(servers.First(), servers.ServerPollingInterval)
                : new RoundRobinServerManager(servers);
        }

        private void Cleanup(object state) {
            var tobeDisposed = new List<ReferenceCountingProviderWrapper>();
            lock(_lock) {
                foreach(var providers in new[] { _poolingProviders, _singleProviders }) {
                    var candidates = providers.Values.Where(x => !x.Active).ToArray();
                    tobeDisposed.AddRange(candidates);
                    foreach(var provider in candidates) {
                        providers.Remove(provider.ServerManager.GetHashCode());
                    }
                }
            }
            foreach(var provider in tobeDisposed) {
                provider.DisposeInternals();
            }
        }
    }
}