﻿using System.Collections.Generic;
using System.Configuration;
using FluentCassandra.Connections;
using Xunit;
using System.Linq;

namespace FluentCassandra.Integration.Tests.Connections
{
    public class ConnectionBuilderTests
    {
        [Fact]
        public void SpacesBeforeServerHostnamesTest()
        {
            // arrange
            IList<Server> expected = new List<Server>();
            expected.Add(new Server("test-host-1"));
            expected.Add(new Server("test-host-2"));
            expected.Add(new Server("test-host-3"));
            string connectionString = string.Format("Keyspace={0};Server={1}, {2}, {3};Pooling=True", ConfigurationManager.AppSettings["TestKeySpace"], expected[0].Host, expected[1].Host, expected[2].Host);
            // act
            var actual = new ConnectionBuilder(connectionString).Servers;

            // assert
            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Server e = expected[i];
                Server a = actual[i];
                Assert.Equal(e.Host, a.Host);
            }
        }

        [Fact]
        public void SpacesAfterServerHostnamesTest()
        {
            // arrange
            IList<Server> expected = new List<Server>();
            expected.Add(new Server("test-host-1"));
            expected.Add(new Server("test-host-2"));
            expected.Add(new Server("test-host-3"));
            string connectionString = string.Format("Keyspace={0};Server={1} ,{2} ,{3};Pooling=True", ConfigurationManager.AppSettings["TestKeySpace"], expected[0].Host, expected[1].Host, expected[2].Host);
            // act
            var actual = new ConnectionBuilder(connectionString).Servers;

            // assert
            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Server e = expected[i];
                Server a = actual[i];
                Assert.Equal(e.Host, a.Host);
            }
        }

        [Fact]
        public void NormalServerHostnamesTest()
        {
            // arrange
            IList<Server> expected = new List<Server>();
            expected.Add(new Server("test-host-1"));
            expected.Add(new Server("test-host-2"));
            expected.Add(new Server("test-host-3"));
            string connectionString = string.Format("Keyspace={0};Server={1},{2},{3};Pooling=True", ConfigurationManager.AppSettings["TestKeySpace"], expected[0].Host, expected[1].Host, expected[2].Host);
            // act
            var actual = new ConnectionBuilder(connectionString).Servers;

            // assert
            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Server e = expected[i];
                Server a = actual.ElementAt(i);
                Assert.Equal(e.Host, a.Host);
            }
        }

        [Fact]
        public void LeadingAndTralingSpacesOnKeyTest()
        {
            // arrange
            string expectedKeyspace = ConfigurationManager.AppSettings["TestKeySpace"];
            IList<Server> expected = new List<Server>();
            expected.Add(new Server("test-host-1"));
            expected.Add(new Server("test-host-2"));
            expected.Add(new Server("test-host-3"));
            string connectionString = string.Format(" Keyspace ={0}; Server ={1},{2},{3}; Pooling =True", ConfigurationManager.AppSettings["TestKeySpace"], expected[0].Host, expected[1].Host, expected[2].Host);
            // act
            var result = new ConnectionBuilder(connectionString);
            var actual = result.Servers;
            string actualKeyspace = result.Keyspace;
            // assert
            Assert.True(result.Pooling);
            Assert.Equal(expectedKeyspace, actualKeyspace);
            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Server e = expected[i];
                Server a = actual.ElementAt(i);
                Assert.Equal(e.Host, a.Host);
            }
        }

        [Fact]
        public void LeadingAndTralingSpacesOnValueTest()
        {
            // arrange
            string expectedKeyspace = ConfigurationManager.AppSettings["TestKeySpace"];
            IList<Server> expected = new List<Server>();
            expected.Add(new Server("test-host-1"));
            expected.Add(new Server("test-host-2"));
            expected.Add(new Server("test-host-3"));
            string connectionString = string.Format("Keyspace= {0} ;Server= {1},{2},{3} ;Pooling= True ", ConfigurationManager.AppSettings["TestKeySpace"], expected[0].Host, expected[1].Host, expected[2].Host);
            // act
            var result = new ConnectionBuilder(connectionString);
            var actual = result.Servers;
            string actualKeyspace = result.Keyspace;
            // assert
            Assert.True(result.Pooling);
            Assert.Equal(expectedKeyspace, actualKeyspace);
            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Server e = expected[i];
                Server a = actual[i];
                Assert.Equal(e.Host, a.Host);
            }
        }
    }
}
