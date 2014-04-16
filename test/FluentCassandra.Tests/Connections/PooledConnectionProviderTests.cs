using System;
using System.Collections.Generic;
using System.Net.Sockets;
using FluentCassandra.Apache.Cassandra;
using Xunit;

namespace FluentCassandra.Connections {
    public class PooledConnectionProviderTests {

        [Fact]
        public void GettingAConnectionFromProviderGetsServerFromServerManager() {
            var connectionFactory = new ConnectionFactoryFake();
            var serverManager = new ServerManagerFake();
            var cluster = new Cluster(new[] { new Server() });
            var pool = new PooledConnectionProvider(serverManager, cluster, connectionFactory.Create);

            var conn = pool.Open();

            Assert.NotNull(conn);
            Assert.Equal(1, connectionFactory.Created.Count);
            Assert.Equal(1, serverManager.GetServerCalled);
        }

        [Fact]
        public void FreeConnectionsAreReturnedBeforeNewConnectionsAreMade() {
            var connectionFactory = new ConnectionFactoryFake();
            var serverManager = new ServerManagerFake();
            var cluster = new Cluster(new[] { new Server() });
            var pool = new PooledConnectionProvider(serverManager, cluster, connectionFactory.Create);

            var conn1 = pool.Open();
            pool.Close(conn1);
            var conn2 = pool.Open();

            Assert.NotNull(conn1);
            Assert.Same(conn1, conn2);
        }

        [Fact]
        public void PoolCreatesNewConnectionsUntilMinPoolsizeIsReached() {
            var connectionFactory = new ConnectionFactoryFake();
            var serverManager = new ServerManagerFake();
            var cluster = new Cluster(new[] { new Server() }, minPoolSize: 2);
            var pool = new PooledConnectionProvider(serverManager, cluster, connectionFactory.Create);

            var conn1 = pool.Open();
            pool.Close(conn1);
            var conn2 = pool.Open();
            pool.Close(conn2);
            var conn3 = pool.Open();

            Assert.NotNull(conn1);
            Assert.NotSame(conn1, conn2);
            Assert.Same(conn1, conn3);
        }

        [Fact]
        public void FailureToOpenConnectionCallsErrorOccuredOnServerManager() {
            var connectionFactory = new ConnectionFactoryFake();
            var createCallback = connectionFactory.CreateCallback;
            connectionFactory.CreateCallback = (server, type, bufferSize) => {
                connectionFactory.CreateCallback = createCallback;
                return new FailingConnectionFake(server);
            };
            var serverManager = new ServerManagerFake();
            var cluster = new Cluster(new[] { new Server() });
            var pool = new PooledConnectionProvider(serverManager, cluster, connectionFactory.Create);

            var conn = pool.Open();
            pool.Close(conn);

            Assert.Equal(1, serverManager.ErrorOccuredCalled);
            Assert.Equal(2, connectionFactory.Created.Count);
        }

        [Fact]
        public void PoolThrowsIfNoServerCanBeRetrievedFromTheServerManager() {
            var connectionFactory = new ConnectionFactoryFake();
            var serverManager = new ServerManagerFake { ServerToReturn = null };
            var cluster = new Cluster(new[] { new Server() });
            var pool = new PooledConnectionProvider(serverManager, cluster, connectionFactory.Create);

            try {
                pool.Open();
                throw new Exception("open did not throw");
            } catch(CassandraException) { }
        }

        private class ConnectionFactoryFake {
            public Func<Server, ConnectionType, int, IConnection> CreateCallback = (server, type, bufferSize) => new ConnectionFake(server);
            public readonly List<IConnection> Created = new List<IConnection>();
            public IConnection Create(Server server, ConnectionType type, int bufferSize) {
                var conn = CreateCallback(server, type, bufferSize);
                Created.Add(conn);
                return conn;
            }
        }

        private class ConnectionFake : IConnection {
            public ConnectionFake(Server server) {
                Server = server;
            }
            public void Dispose() { }
            public DateTime Created { get; private set; }
            public virtual bool IsOpen { get { return true; } }
            public Server Server { get; private set; }
            public Cassandra.Client Client { get; private set; }
            public void SetKeyspace(string keyspace) { }
            public void SetCqlVersion(string cqlVersion) { }
            public virtual void Open() { }
            public void Close() { }
        }

        private class FailingConnectionFake : ConnectionFake {
            public FailingConnectionFake(Server server) : base(server) { }
            public override bool IsOpen { get { return false; } }
            public override void Open() {
                throw new SocketException();
            }
        }

        private class ServerManagerFake : IServerManager {

            public int ErrorOccuredCalled = 0;
            public Server ServerToReturn = new Server();
            public int GetServerCalled = 0;
            public void Dispose() { }
            public void ErrorOccurred(Server server, Exception exc = null) {
                ErrorOccuredCalled++;
            }
            public Server GetServer() {
                GetServerCalled++;
                return ServerToReturn;
            }
        }
    }
}