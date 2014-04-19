using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FluentCassandra.Connections {

    /// <summary>
    /// Note: This factory wraps every Provider with a disposable closure, so that the providers (and thereby their server and connection pools)
    /// can be shared, but can still be cleaned up when no longer needed. Should it be desired to handle managment explicitly with a determinable
    /// resources disposal, the connection providers should be created and managed manually and provided directly to the constructor of
    /// CassandraContext
    /// </summary>
    public static class ConnectionProviderFactory {

        private class DisposableProviderWrapper : IConnectionProvider {

            private readonly IConnectionProvider _connectionProvider;
            private readonly Action _disposalCallback;
            private bool _isDisposed;

            public DisposableProviderWrapper(IConnectionProvider connectionProvider, Action disposalCallback) {
                _connectionProvider = connectionProvider;
                _disposalCallback = disposalCallback;
            }

            public void Dispose() {
                if(_isDisposed) {
                    return;
                }
                _isDisposed = true;
                _disposalCallback();
            }

            public IConnection Open() {
                EnsureNotDisposed();
                return _connectionProvider.Open();
            }

            public void ErrorOccurred(IConnection connection, Exception exc = null) {
                EnsureNotDisposed();
                _connectionProvider.ErrorOccurred(connection, exc);
            }

            public void Close(IConnection connection) {
                EnsureNotDisposed();
                _connectionProvider.Close(connection);
            }

            private void EnsureNotDisposed() {
                if(_isDisposed) {
                    throw new ObjectDisposedException("DisposableProviderWrapper:" + _connectionProvider.GetType().Name);
                }
            }
        }

        private abstract class AProviderReferenceManager {
            private readonly IConnectionProvider _provider;
            private readonly IServerManager _serverManager;
            private volatile int _refs;

            protected AProviderReferenceManager(Cluster cluster) {
                _serverManager = cluster.Count() == 1
                    ? (IServerManager)new SingleServerManager(cluster.First(), cluster.ServerPollingInterval)
                    : new RoundRobinServerManager(cluster);
                _provider = CreateProvider(cluster, _serverManager);
            }

            protected abstract IConnectionProvider CreateProvider(Cluster cluster, IServerManager serverManager);

            public IConnectionProvider CreateDisposable() {
                Interlocked.Increment(ref _refs);
                return new DisposableProviderWrapper(_provider, Decrement);
            }

            private void Decrement() {
                Interlocked.Decrement(ref _refs);
            }

            public bool IsActive { get { return _refs == 0; } }

            public void Dispose() {
                _provider.Dispose();
                _serverManager.Dispose();
            }
        }

        private class PoolingProviderReferenceManager : AProviderReferenceManager {
            public PoolingProviderReferenceManager(Cluster cluster) : base(cluster) { }
            protected override IConnectionProvider CreateProvider(Cluster cluster, IServerManager serverManager) {
                return new PooledConnectionProvider(serverManager, cluster);
            }
        }
        private class NormalProviderReferenceManager : AProviderReferenceManager {
            public NormalProviderReferenceManager(Cluster cluster) : base(cluster) { }
            protected override IConnectionProvider CreateProvider(Cluster cluster, IServerManager serverManager) {
                return new NormalConnectionProvider(serverManager, cluster);
            }
        }

        private static readonly object _lock = new object();
        private static readonly Dictionary<int, AProviderReferenceManager> _poolingProviderManagers = new Dictionary<int, AProviderReferenceManager>();
        private static readonly Dictionary<int, AProviderReferenceManager> _singleProviderManagers = new Dictionary<int, AProviderReferenceManager>();

        static ConnectionProviderFactory() {
            new Timer(Cleanup, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        }

        public static IConnectionProvider Get(IConnectionBuilder builder) {
            lock(_lock) {
                var cluster = builder.Cluster;
                var hashcode = cluster.GetHashCode();
                AProviderReferenceManager manager;
                if(cluster.Pooling) {
                    if(!_poolingProviderManagers.TryGetValue(hashcode, out manager)) {
                        _poolingProviderManagers[hashcode] = manager = new PoolingProviderReferenceManager(cluster);
                    }
                } else {
                    if(!_singleProviderManagers.TryGetValue(hashcode, out manager)) {
                        _poolingProviderManagers[hashcode] = manager = new NormalProviderReferenceManager(cluster);
                    }
                }
                return manager.CreateDisposable();
            }
        }

        private static void Cleanup(object state) {
            var tobeDisposed = new List<AProviderReferenceManager>();
            lock(_lock) {
                foreach(var managers in new[] { _poolingProviderManagers, _poolingProviderManagers }) {
                    var candidates = managers.Where(x => !x.Value.IsActive);
                    tobeDisposed.AddRange(candidates.Select(x => x.Value));
                    foreach(var manager in candidates) {
                        managers.Remove(manager.Key);
                    }
                }
            }
            foreach(var provider in tobeDisposed) {
                provider.Dispose();
            }
        }
    }
}