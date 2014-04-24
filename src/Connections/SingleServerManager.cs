using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace FluentCassandra.Connections {
    public class SingleServerManager : IServerManager {
        private readonly object _lock = new object();
        private readonly Timer _recoveryTimer;
        private readonly long _recoveryTimerInterval;
        private readonly Server _server;
        private bool _failed;
        private bool _isDisposed;

        public SingleServerManager(Server server, TimeSpan pollingInterval) {
            _server = server;
            _recoveryTimerInterval = (long)pollingInterval.TotalMilliseconds;
            _recoveryTimer = new Timer(ServerRecover);
        }

        private void ServerRecover(object unused) {
            if(_isDisposed) {
                return;
            }
            lock(_lock) {
                if(!_failed)
                    return;

                var connection = new Connection(_server, ConnectionType.Simple, 1024);

                try {
                    connection.Open();
                    _failed = false;
                    connection.Close();
                } catch { }
            }
        }

        #region IServerManager Members

        public void ErrorOccurred(Server server, Exception exc = null) {
            if(_isDisposed) {
                return;
            }
            Debug.WriteLineIf(exc != null, exc, "connection");
            lock(_lock) {
                if(_failed)
                    return;

                _failed = true;
                _recoveryTimer.Change(_recoveryTimerInterval, Timeout.Infinite);
            }
        }

        public Server GetServer() {
            return _failed ? null : _server;
        }
        #endregion

        public void Dispose() {
            if(_isDisposed) {
                return;
            }
            _isDisposed = true;
            _recoveryTimer.Dispose();
        }
    }
}