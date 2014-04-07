using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Connections {

    public class SingleServerManagerTests {

        [Fact]
        public void GetServerReturnsNullAfterServerFailure() {
            var builder = new ConnectionBuilder("Server=unit-test-1");
            var manager = new SingleServerManager(builder.Servers.First(), builder.ServerPollingInterval);

            var server = manager.GetServer();
            Assert.False(manager.GetServer() == null, "SingleServerManager was not initialized with a server");
            manager.ErrorOccurred(server, new Exception());

            Assert.True(manager.GetServer() == null, "SingleServerManager still has a server after its failure");
        }
    }
}