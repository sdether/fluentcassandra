using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Connections {
    public class Cluster : IEnumerable<Server>, IEquatable<Cluster> {
        private readonly Server[] _servers;
        private readonly int _hashCode;
        public readonly bool Pooling;
        public readonly TimeSpan ServerPollingInterval;
        public readonly int MinPoolSize;
        public readonly int MaxPoolSize;
        public readonly TimeSpan ConnectionTimeout;
        public readonly ConnectionType ConnectionType;
        public readonly TimeSpan ConnectionLifetime;
        public readonly int BufferSize;

        public Cluster(IEnumerable<Server> servers, bool pooling = false, TimeSpan? connectionTimeout = null, int minPoolSize = 0, int maxPoolSize = 100, TimeSpan? serverPollingInterval = null, TimeSpan? connectionLifetime = null, ConnectionType connectionType = ConnectionType.Framed, int bufferSize = 1024) {
            _servers = servers.ToArray();
            Pooling = pooling;
            MinPoolSize = minPoolSize;
            MaxPoolSize = maxPoolSize;
            ServerPollingInterval = serverPollingInterval ?? TimeSpan.FromSeconds(30);
            ConnectionLifetime = connectionLifetime ?? TimeSpan.Zero;
            ConnectionType = connectionType;
            ConnectionTimeout = connectionTimeout ?? TimeSpan.FromSeconds(Server.DefaultTimeout);
            BufferSize = bufferSize;
            _hashCode = (_servers.Select(x => x.ToString()).OrderBy(x => x).Aggregate((x, y) => x + y)
                    + Pooling
                    + MinPoolSize
                    + MaxPoolSize
                    + ServerPollingInterval
                    + ConnectionLifetime
                    + ConnectionTimeout
                    + ConnectionType
                    + BufferSize
                ).GetHashCode();
        }

        public int Count { get { return _servers.Length; } }
        public Server this[int index] { get { return _servers[index]; } }

        public IEnumerator<Server> GetEnumerator() {
            return ((IEnumerable<Server>)_servers).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        public bool Equals(Cluster other) {
            return _hashCode == other._hashCode;
        }
        public override bool Equals(object obj) {
            return obj is Cluster && Equals((Cluster)obj);
        }

        public override int GetHashCode() {
            return _hashCode;
        }

        public Cluster WithServers(IEnumerable<Server> servers) {
            return new Cluster(servers, Pooling, ConnectionTimeout, MinPoolSize, MaxPoolSize, ServerPollingInterval, ConnectionLifetime, ConnectionType, BufferSize);
        }
    }
}