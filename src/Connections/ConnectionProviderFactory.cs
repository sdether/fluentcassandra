using System;
using System.Collections.Generic;
using System.Threading;

namespace FluentCassandra.Connections {
    public class ConnectionProviderFactory {

        private static readonly object _lock = new object();
        private static readonly IServerManagerFactory _serverManagerFactory;
        private static readonly Dictionary<int, ReferenceCountingProviderWrapper> _poolingProviders = new Dictionary<int, ReferenceCountingProviderWrapper>();
        private static readonly Dictionary<int, ReferenceCountingProviderWrapper> _singleProviders = new Dictionary<int, ReferenceCountingProviderWrapper>();

        public static IConnectionProvider Get(IConnectionBuilder builder) {
            lock(_lock) {
                ReferenceCountingProviderWrapper provider;
                var hashcode = builder.Servers.GetHashCode();
                if(builder.Pooling) {
                    if(_poolingProviders.TryGetValue(hashcode, out provider) && provider.Active) {
                        provider.IncrementReferences();
                    } else {
                        provider = new ReferenceCountingProviderWrapper(new PooledConnectionProvider(_serverManagerFactory.Get(builder.GetServerCollection()), builder));
                        _poolingProviders[hashcode] = provider;
                    }
                } else {
                    if(_singleProviders.TryGetValue(hashcode, out provider) && provider.Active) {
                        provider.IncrementReferences();
                    } else {
                        provider = new ReferenceCountingProviderWrapper(new PooledConnectionProvider(_serverManagerFactory.Get(builder.GetServerCollection()), builder));
                        _singleProviders[hashcode] = provider;
                    }
                }
                return provider;
            }
        }

        private class ReferenceCountingProviderWrapper : IConnectionProvider {
            private readonly IConnectionProvider _connectionProvider;
            private int _references;

            public ReferenceCountingProviderWrapper(IConnectionProvider connectionProvider) {
                _connectionProvider = connectionProvider;
                _references = 1;
            }

            public bool Active {
                get { return _references > 0; }
            }

            public void IncrementReferences() {
                Interlocked.Increment(ref _references);
            }

            public void Dispose() {
                var references = Interlocked.Decrement(ref _references);
                if(_references <= 0) {
                    _connectionProvider.Dispose();
                }
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
        }
    }
}