using System.Collections;
using System.Collections.ObjectModel;
using FluentCassandra.Apache.Cassandra;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace FluentCassandra.Connections {
    public class ConnectionBuilder : FluentCassandra.Connections.IConnectionBuilder {

        private readonly ObservableServerList _observableServerList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keyspace"></param>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="timeout"></param>
        public ConnectionBuilder(string keyspace, string host, int port = Server.DefaultPort, int connectionTimeout = Server.DefaultTimeout, bool pooling = false, int minPoolSize = 0, int maxPoolSize = 100, int maxRetries = 0, int serverPollingInterval = 30, int connectionLifetime = 0, ConnectionType connectionType = ConnectionType.Framed, int bufferSize = 1024, ConsistencyLevel read = ConsistencyLevel.QUORUM, ConsistencyLevel write = ConsistencyLevel.QUORUM, string cqlVersion = FluentCassandra.Connections.CqlVersion.Edge, bool compressCqlQueries = true, string username = null, string password = null)
            : this() {
            Keyspace = keyspace;
            MaxRetries = maxRetries;
            ReadConsistency = read;
            WriteConsistency = write;
            CqlVersion = cqlVersion;
            CompressCqlQueries = compressCqlQueries;
            Username = username;
            Password = password;
            Cluster = new Cluster(new[] { new Server(host, port) }, pooling, TimeSpan.FromSeconds(connectionTimeout), minPoolSize, maxPoolSize, TimeSpan.FromSeconds(serverPollingInterval), TimeSpan.FromSeconds(connectionLifetime), connectionType, bufferSize);

            ConnectionString = GetConnectionString();
        }

        public ConnectionBuilder(string keyspace, Server server, bool pooling = false, int minPoolSize = 0, int maxPoolSize = 100, int maxRetries = 0, int serverPollingInterval = 30, int connectionLifetime = 0, ConnectionType connectionType = ConnectionType.Framed, int bufferSize = 1024, ConsistencyLevel read = ConsistencyLevel.QUORUM, ConsistencyLevel write = ConsistencyLevel.QUORUM, string cqlVersion = FluentCassandra.Connections.CqlVersion.Edge, bool compressCqlQueries = true, string username = null, string password = null)
            : this() {
            Keyspace = keyspace;
            MaxRetries = maxRetries;
            ReadConsistency = read;
            WriteConsistency = write;
            CqlVersion = cqlVersion;
            CompressCqlQueries = compressCqlQueries;
            Username = username;
            Password = password;
            Cluster = new Cluster(new[] { server }, pooling, TimeSpan.FromSeconds(server.Timeout), minPoolSize, maxPoolSize, TimeSpan.FromSeconds(serverPollingInterval), TimeSpan.FromSeconds(connectionLifetime), connectionType, bufferSize);

            ConnectionString = GetConnectionString();
        }

        public ConnectionBuilder(string keyspace, IEnumerable<Server> servers, bool pooling = false, int minPoolSize = 0, int maxPoolSize = 100, int maxRetries = 0, int serverPollingInterval = 30, int connectionLifetime = 0, ConnectionType connectionType = ConnectionType.Framed, int bufferSize = 1024, ConsistencyLevel read = ConsistencyLevel.QUORUM, ConsistencyLevel write = ConsistencyLevel.QUORUM, string cqlVersion = FluentCassandra.Connections.CqlVersion.Edge, bool compressCqlQueries = true, string username = null, string password = null)
            : this() {
            Keyspace = keyspace;
            MaxRetries = maxRetries;
            ReadConsistency = read;
            WriteConsistency = write;
            CqlVersion = cqlVersion;
            CompressCqlQueries = compressCqlQueries;
            Username = username;
            Password = password;
            Cluster = new Cluster(servers, pooling, TimeSpan.FromSeconds(servers.First().Timeout), minPoolSize, maxPoolSize, TimeSpan.FromSeconds(serverPollingInterval), TimeSpan.FromSeconds(connectionLifetime), connectionType, bufferSize);

            ConnectionString = GetConnectionString();
        }

        public ConnectionBuilder(string keyspace, Cluster cluster, int maxRetries = 0, ConsistencyLevel read = ConsistencyLevel.QUORUM, ConsistencyLevel write = ConsistencyLevel.QUORUM, string cqlVersion = FluentCassandra.Connections.CqlVersion.Edge, bool compressCqlQueries = true, string username = null, string password = null)
            : this() {
            Keyspace = keyspace;
            MaxRetries = maxRetries;
            ReadConsistency = read;
            WriteConsistency = write;
            CqlVersion = cqlVersion;
            CompressCqlQueries = compressCqlQueries;
            Username = username;
            Password = password;
            Cluster = cluster;

            ConnectionString = GetConnectionString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        public ConnectionBuilder(string connectionString)
            : this() {
            InitializeConnectionString(connectionString);
            ConnectionString = GetConnectionString();
        }

        private ConnectionBuilder() {
            _observableServerList = new ObservableServerList(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        private void InitializeConnectionString(string connectionString) {
            string[] connParts = connectionString.Split(';');
            IDictionary<string, string> pairs = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

            foreach(string part in connParts) {
                string[] nameValue = part.Split(new[] { '=' }, 2);

                if(nameValue.Length != 2)
                    continue;

                pairs.Add(nameValue[0].Trim(), nameValue[1].Trim());
            }

            #region Keyspace

            if(pairs.ContainsKey("Keyspace")) {
                Keyspace = pairs["Keyspace"];
            }

            #endregion

            #region ConnectionTimeout
            TimeSpan connectionTimeout = TimeSpan.Zero;
            if(pairs.ContainsKey("Connection Timeout")) {
                int connectionTimeoutInt;

                if(!Int32.TryParse(pairs["Connection Timeout"], out connectionTimeoutInt))
                    throw new CassandraException("Connection Timeout is not valid.");

                if(connectionTimeoutInt < 0)
                    connectionTimeoutInt = 0;

                connectionTimeout = TimeSpan.FromSeconds(connectionTimeoutInt);
            }

            #endregion

            #region Pooling
            var pooling = false;
            if(pairs.ContainsKey("Pooling")) {
                Boolean.TryParse(pairs["Pooling"], out pooling);
            }

            #endregion

            #region MinPoolSize
            var minPoolSize = 0;
            if(pairs.ContainsKey("Min Pool Size")) {
                Int32.TryParse(pairs["Min Pool Size"], out minPoolSize);

                if(minPoolSize < 0)
                    minPoolSize = 0;

            }

            #endregion

            #region MaxPoolSize
            var maxPoolSize = 100;
            if(pairs.ContainsKey("Max Pool Size")) {
                Int32.TryParse(pairs["Max Pool Size"], out maxPoolSize);

                if(maxPoolSize < 0)
                    maxPoolSize = 100;

            }

            #endregion

            #region MaxRetries

            if(pairs.ContainsKey("Max Retries")) {
                int maxRetries;

                if(!Int32.TryParse(pairs["Max Retries"], out maxRetries))
                    maxRetries = 0;

                if(maxRetries < 0)
                    maxRetries = 0;

                MaxRetries = maxRetries;
            }

            #endregion

            #region ServerRecoveryInterval
            var serverPollingInterval = TimeSpan.Zero;
            if(pairs.ContainsKey("Server Polling Interval")) {
                int serverPollingIntervalInt;

                if(!Int32.TryParse(pairs["Server Polling Interval"], out serverPollingIntervalInt))
                    serverPollingIntervalInt = 0;

                if(serverPollingIntervalInt < 0)
                    serverPollingIntervalInt = 0;

                serverPollingInterval = TimeSpan.FromSeconds(serverPollingIntervalInt);
            }

            #endregion

            #region ConnectionLifetime
            var connectionLifetime = TimeSpan.Zero;
            if(pairs.ContainsKey("Connection Lifetime")) {
                int lifetime;

                if(!Int32.TryParse(pairs["Connection Lifetime"], out lifetime))
                    lifetime = 0;

                if(lifetime < 0)
                    lifetime = 0;

                connectionLifetime = TimeSpan.FromSeconds(lifetime);
            }

            #endregion

            #region ConnectionType
            var connectionType = ConnectionType.Framed;
            if(pairs.ContainsKey("Connection Type")) {

                Enum.TryParse(pairs["Connection Type"], out connectionType);
            }

            #endregion

            #region BufferSize
            int bufferSize = 1024;
            if(pairs.ContainsKey("Buffer Size")) {

                Int32.TryParse(pairs["Buffer Size"], out bufferSize);

                if(bufferSize < 0)
                    bufferSize = 1024;
            }

            #endregion

            #region Read

            if(!pairs.ContainsKey("Read")) {
                ReadConsistency = ConsistencyLevel.QUORUM;
            } else {
                ConsistencyLevel read;

                if(!Enum.TryParse(pairs["Read"], out read))
                    ReadConsistency = ConsistencyLevel.QUORUM;

                ReadConsistency = read;
            }

            #endregion

            #region Write

            if(!pairs.ContainsKey("Write")) {
                WriteConsistency = ConsistencyLevel.QUORUM;
            } else {
                ConsistencyLevel write;

                if(!Enum.TryParse(pairs["Write"], out write))
                    WriteConsistency = ConsistencyLevel.QUORUM;

                WriteConsistency = write;
            }

            #endregion

            #region CqlVersion

            if(!pairs.ContainsKey("CQL Version")) {
                CqlVersion = FluentCassandra.Connections.CqlVersion.Edge;
            } else {
                CqlVersion = pairs["CQL Version"];
            }

            #endregion

            #region CompressCqlQueries

            if(!pairs.ContainsKey("Compress CQL Queries")) {
                CompressCqlQueries = true;
            } else {
                string compressCqlQueriesValue = pairs["Compress CQL Queries"];

                // YES or TRUE is a positive response everything else is a negative response
                CompressCqlQueries = String.Equals("yes", compressCqlQueriesValue, StringComparison.OrdinalIgnoreCase) || String.Equals("true", compressCqlQueriesValue, StringComparison.OrdinalIgnoreCase);
            }

            #endregion

            #region Username

            if(pairs.ContainsKey("Username")) {
                Username = pairs["Username"];
            }

            #endregion

            #region Password

            if(pairs.ContainsKey("Password")) {
                Password = pairs["Password"];
            }

            #endregion

            // This must be last because it uses fields from above
            #region Server

            var servers = new List<Server>();

            if(!pairs.ContainsKey("Server")) {
                Servers.Add(new Server());
            } else {
                string[] serverStrings = pairs["Server"].Split(',');
                foreach(var server in serverStrings) {
                    string[] serverParts = server.Split(':');
                    string host = serverParts[0].Trim();

                    if(serverParts.Length == 2) {
                        int port;
                        if(Int32.TryParse(serverParts[1].Trim(), out port))
                            servers.Add(new Server(host: host, port: port, timeout: connectionTimeout.Seconds));
                        else
                            servers.Add(new Server(host: host, timeout: connectionTimeout.Seconds));
                    } else
                        servers.Add(new Server(host: host, timeout: connectionTimeout.Seconds));
                }
            }
            Cluster = new Cluster(servers, pooling, connectionTimeout, minPoolSize, maxPoolSize, serverPollingInterval, connectionLifetime, connectionType, bufferSize);
            #endregion
        }

        private string GetConnectionString() {
            StringBuilder b = new StringBuilder();
            string format = "{0}={1};";

            b.AppendFormat(format, "Keyspace", Keyspace);
            b.AppendFormat(format, "Server", String.Join(",", Servers));

            b.AppendFormat(format, "Pooling", Pooling);
            b.AppendFormat(format, "Min Pool Size", MinPoolSize);
            b.AppendFormat(format, "Max Pool Size", MaxPoolSize);
            b.AppendFormat(format, "Max Retries", MaxRetries);
            b.AppendFormat(format, "Connection Timeout", Convert.ToInt32(ConnectionTimeout.TotalSeconds));
            b.AppendFormat(format, "Connection Lifetime", Convert.ToInt32(ConnectionLifetime.TotalSeconds));
            b.AppendFormat(format, "Server Recovery Interval", Convert.ToInt32(ServerPollingInterval.TotalSeconds));
            b.AppendFormat(format, "Connection Type", ConnectionType);

            b.AppendFormat(format, "Buffer Size", BufferSize);
            b.AppendFormat(format, "Read", ReadConsistency);
            b.AppendFormat(format, "Write", WriteConsistency);

            b.AppendFormat(format, "CQL Version", CqlVersion);
            b.AppendFormat(format, "Compress CQL Queries", CompressCqlQueries);

            b.AppendFormat(format, "Username", Username);
            b.AppendFormat(format, "Password", Password);

            return b.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        public string Keyspace { get; private set; }

        /// <summary>
        /// The length of time (in seconds) to wait for a connection to the server before terminating the attempt and generating an error.
        /// </summary>
        public TimeSpan ConnectionTimeout { get { return Cluster.ConnectionTimeout; } }

        /// <summary>
        /// When true, the Connection object is drawn from the appropriate pool, or if necessary, is created and added to the appropriate pool. Recognized values are true, false, yes, and no.
        /// </summary>
        public bool Pooling { get { return Cluster.Pooling; } }

        /// <summary>
        /// (Not Currently Implimented) The minimum number of connections allowed in the pool.
        /// </summary>
        public int MinPoolSize { get { return Cluster.MinPoolSize; } }

        /// <summary>
        /// The maximum number of connections allowed in the pool.
        /// </summary>
        public int MaxPoolSize { get { return Cluster.MaxPoolSize; } }

        /// <summary>
        /// The maximum number of execution retry attempts if there is an error during the execution of an operation and the exception is a type that can be retried.
        /// </summary>
        public int MaxRetries { get; private set; }

        /// <summary>
        /// When a connection is returned to the pool, its creation time is compared with the current time, and the connection is destroyed if that time span (in seconds) exceeds the value specified by Connection Lifetime. This is useful in clustered configurations to force load balancing between a running server and a server just brought online. A value of zero (0) causes pooled connections to have the maximum connection timeout.
        /// </summary>
        public TimeSpan ConnectionLifetime { get { return Cluster.ConnectionLifetime; } }

        /// <summary>
        /// 
        /// </summary>
        public ConnectionType ConnectionType { get { return Cluster.ConnectionType; } }

        /// <summary>
        /// 
        /// </summary>
        public TimeSpan ServerPollingInterval { get { return Cluster.ServerPollingInterval; } }
        /// <summary>
        /// 
        /// </summary>
        public int BufferSize { get { return Cluster.BufferSize; } }

        /// <summary>
        /// 
        /// </summary>
        public ConsistencyLevel ReadConsistency { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public ConsistencyLevel WriteConsistency { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public IList<Server> Servers { get { return _observableServerList; } }

        /// <summary>
        /// 
        /// </summary>
        public string CqlVersion { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public bool CompressCqlQueries { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public string Username { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public string ConnectionString { get; private set; }

        /// <summary>
        /// A unique identifier for the connection builder.
        /// </summary>
        public string Uuid { get { return ConnectionString; } }

        public Cluster Cluster { get; private set; }

        private class ObservableServerList : IList<Server> {
            private readonly ConnectionBuilder _this;

            public ObservableServerList(ConnectionBuilder @this) {
                _this = @this;
            }

            public IEnumerator<Server> GetEnumerator() {
                return _this.Cluster.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator() {
                return GetEnumerator();
            }

            public void Add(Server item) {
                _this.Cluster = _this.Cluster.WithServers(_this.Cluster.Concat(new[] { item }));
                _this.ConnectionString = _this.GetConnectionString();
            }

            public void Clear() {
                _this.Cluster = _this.Cluster.WithServers(new Server[0]);
                _this.ConnectionString = _this.GetConnectionString();
            }

            public bool Contains(Server item) {
                return _this.Cluster.Contains(item);
            }

            public void CopyTo(Server[] array, int arrayIndex) {
                var servers = _this.Cluster.ToArray();
                servers.CopyTo(array, arrayIndex);
            }

            public bool Remove(Server item) {
                var servers = _this.Cluster.ToList();
                var removed = servers.Remove(item);
                if(!removed) {
                    return false;
                }
                _this.Cluster = _this.Cluster.WithServers(servers);
                _this.ConnectionString = _this.GetConnectionString();
                return true;
            }

            public int Count { get { return _this.Cluster.Count; } }
            public bool IsReadOnly { get { return false; } }
            public int IndexOf(Server item) {
                return Array.IndexOf(_this.Cluster.ToArray(), item);
            }

            public void Insert(int index, Server item) {
                var servers = _this.Cluster.ToList();
                servers.Insert(index, item);
                _this.Cluster = _this.Cluster.WithServers(servers);
                _this.ConnectionString = _this.GetConnectionString();
            }

            public void RemoveAt(int index) {
                var servers = _this.Cluster.ToList();
                servers.RemoveAt(index);
                _this.Cluster = _this.Cluster.WithServers(servers);
                _this.ConnectionString = _this.GetConnectionString();
            }

            public Server this[int index] {
                get { return _this.Cluster[index]; }
                set {
                    var servers = _this.Cluster.ToList();
                    servers[index] = value;
                    _this.Cluster = _this.Cluster.WithServers(servers);
                    _this.ConnectionString = _this.GetConnectionString();
                }
            }
        }
    }
}