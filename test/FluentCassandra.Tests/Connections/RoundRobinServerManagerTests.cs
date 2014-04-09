using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FluentCassandra.Connections
{
	public class RoundRobinServerManagerTests
	{
		[Fact]
		public void CanBlackListAndCleanQueueTest()
		{
            var connectionBuilder = new ConnectionBuilder("Server=unit-test-1");
            var srv = new Server("unit-test-4");
            connectionBuilder.Servers.Add(srv);
            var target = new RoundRobinServerManager(connectionBuilder.Cluster);

			bool gotServer4 = false;

			for (var i = 0; i < 4; i++) {
			    var server = target.GetServer();
				if (server.ToString().Equals(srv.ToString(), StringComparison.OrdinalIgnoreCase))
				{
					gotServer4 = true;
					break;
				}
			}

			Assert.True(gotServer4);

			target.ErrorOccurred(srv);

			gotServer4 = false;
			for (var i = 0; i < 4; i++)
			{
                var server = target.GetServer();
                if(server.Equals(srv))
				{
					gotServer4 = true;
					break;
				}
			}

			Assert.False(gotServer4);
		}

		[Fact]
		public void HasNextWithMoreThanHalfBlacklistedTest() {
		    var connectionBuilder = new ConnectionBuilder("Server=unit-test-1");
            Server srv1 = connectionBuilder.Servers.First();
            var srv2 = new Server("unit-test-2");
            var srv3 = new Server("unit-test-3");
            var srv4 = new Server("unit-test-4");
            connectionBuilder.Servers.Add(srv2);
            connectionBuilder.Servers.Add(srv3);
            connectionBuilder.Servers.Add(srv4);
            var target = new RoundRobinServerManager(connectionBuilder.Cluster);

			var servers = new List<Server> { new Server("unit-test-1"), srv2, srv3, srv4 };

			for (var i = 0; i < 4; i++) {
			    var srv = target.GetServer();
				Assert.True(connectionBuilder.Servers[i].ToString().Equals(srv.ToString(), StringComparison.OrdinalIgnoreCase));
			}

			target.ErrorOccurred(srv2);
			target.ErrorOccurred(srv3);
			Assert.NotNull(target.GetServer());

			target.ErrorOccurred(srv1);
            Assert.NotNull(target.GetServer());

			target.ErrorOccurred(srv4);
            Assert.Null(target.GetServer());
        }
	}
}
