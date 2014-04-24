using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace FluentCassandra.Connections {
    public class RoundRobinServerManager : IServerManager {
        private readonly object _lock = new object();
        private readonly HashSet<Server> _blackListed;
        private readonly Timer _recoveryTimer;
        private readonly long _recoveryTimerInterval;
        private bool _isDisposed;
        private readonly List<Server> _live;
        private int _index = -1;

        public RoundRobinServerManager(Cluster cluster) {
            _live = new List<Server>(cluster);
            _blackListed = new HashSet<Server>();
            _recoveryTimerInterval = (long)cluster.ServerPollingInterval.TotalMilliseconds;
            _recoveryTimer = new Timer(o => ServerRecover(), null, _recoveryTimerInterval, Timeout.Infinite);
        }

        private void ServerRecover() {
            if(_isDisposed) {
                return;
            }

            try {
                if(_blackListed.Count <= 0) {
                    return;
                }
                Server[] clonedBlackList;

                lock(_lock) {
                    clonedBlackList = _blackListed.ToArray();
                }
                foreach(var server in clonedBlackList) {
                    var connection = new Connection(server, ConnectionType.Simple, 1024);

                    try {
                        connection.Open();
                        lock(_lock) {
                            _blackListed.Remove(server);
                            _live.Add(server);
                        }
                        connection.Close();
                    } catch { }
                }
            } finally {
                _recoveryTimer.Change(_recoveryTimerInterval, Timeout.Infinite);
            }
        }


        #region IServerManager Members

        public void ErrorOccurred(Server server, Exception exc = null) {
            if(_isDisposed) {
                return;
            }
            Debug.WriteLineIf(exc != null, exc, "connection");
            Debug.WriteLine(server + " has been blacklisted", "connection");

            lock(_lock) {
                if(_blackListed.Add(server)) {
                    _live.Remove(server);
                }
            }
        }

        public Server GetServer() {
            if(_isDisposed) {
                throw new ObjectDisposedException("RoundRobinServerManager", "RoundRobinServerManager has already been disposed");
            }
            lock(_lock) {
                if(!_live.Any()) {
                    return null;
                }
                _index++;
                if(_index >= _live.Count) {
                    _index = 0;
                }
                return _live[_index];
            }
        }
        #endregion

        #region IDisposable Members

        public void Dispose() {
            if(_isDisposed) {
                return;
            }
            _isDisposed = true;
            _recoveryTimer.Dispose();
        }

        #endregion
    }
}