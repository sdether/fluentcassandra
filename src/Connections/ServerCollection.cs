using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Connections {
    public class ServerCollection : IEnumerable<Server>, IEquatable<ServerCollection> {
        private readonly Server[] _servers;
        private readonly int _hashCode;
        public readonly TimeSpan ServerPollingInterval;

        public ServerCollection(IEnumerable<Server> servers, TimeSpan serverPollingInterval) {
            _servers = servers.ToArray();
            _hashCode = _servers.Select(x => x.ToString()).OrderBy(x => x).Aggregate((x, y) => x + y).GetHashCode();
            ServerPollingInterval = serverPollingInterval;
        }

        public int Count { get { return _servers.Length; } }
        public Server this[int index] { get { return _servers[index]; } }

        public IEnumerator<Server> GetEnumerator() {
            return ((IEnumerable<Server>)_servers).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        public bool Equals(ServerCollection other) {
            return _hashCode == other._hashCode;
        }
        public override bool Equals(object obj) {
            return obj is ServerCollection && Equals((ServerCollection)obj);
        }

        public override int GetHashCode() {
            return _hashCode;
        }
    }
}