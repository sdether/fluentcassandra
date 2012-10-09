using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentCassandra.Connections;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra.Sandbox {

    [TestFixture]
    public class UserActivityTests {

        public static readonly Server Server = new Server(ConfigurationManager.AppSettings["TestServer"]);
        public static readonly string KeyspaceName = "mt_log";

        [Test]
        public void Socket_failure() {
            var keyspaceName = "test";
            var connectionBuilder = new ConnectionBuilder("test", Server, true);
            var id = 1;
            try {
                using(var db = new CassandraContext(connectionBuilder)) {
                    if(db.KeyspaceExists(keyspaceName))
                        db.DropKeyspace(keyspaceName);
                    var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema {
                        Name = keyspaceName,
                    }, db);
                    keyspace.TryCreateSelf();
                    if(!keyspace.ColumnFamilyExists("event")) {
                        keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
                            FamilyName = "event",
                            KeyValueType = CassandraType.IntegerType,
                            ColumnNameType = CassandraType.UTF8Type,
                            DefaultColumnValueType = CassandraType.UTF8Type,
                            Columns = { new CassandraColumnSchema() { Name = "Name" } }
                        });
                    }
                    while(true) {
                        var evFamily = db.GetColumnFamily("event");
                        dynamic ev = evFamily.CreateRecord(new BigInteger(id));
                        db.Attach(ev);
                        ev.Name = "view " + id;
                        db.SaveChanges();
                        id++;
                        if(id % 1000 == 0) {
                            Console.WriteLine("created {0} users ", id);
                        }
                    }
                }
            } catch {
                Console.WriteLine("died after {0} ops", id);
                throw;
            }
        }

        [Test]
        public void Socket_failure_multithreaded() {
            var keyspaceName = "test";
            var connectionBuilder = new ConnectionBuilder("test", Server, true);
            using(var db = new CassandraContext(connectionBuilder)) {
                if(db.KeyspaceExists(keyspaceName))
                    db.DropKeyspace(keyspaceName);
                var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema {
                    Name = keyspaceName,
                }, db);
                keyspace.TryCreateSelf();
                if(!keyspace.ColumnFamilyExists("event")) {
                    keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
                        FamilyName = "event",
                        KeyValueType = CassandraType.IntegerType,
                        ColumnNameType = CassandraType.UTF8Type,
                        DefaultColumnValueType = CassandraType.UTF8Type,
                        Columns = { new CassandraColumnSchema() { Name = "Name" } }
                    });
                }
            }
            var id = 1;
            var workerCount = 20;
            var workers = new List<Task>();
            for(var i = 0; i < workerCount; i++) {
                workers.Add(Task.Factory.StartNew(() => {
                    try {
                        using(var db = new CassandraContext(connectionBuilder)) {
                            while(true) {
                                Thread.Sleep(50);
                                var evFamily = db.GetColumnFamily("event");
                                dynamic ev = evFamily.CreateRecord(new BigInteger(id));
                                db.Attach(ev);
                                ev.Name = "view " + id;
                                db.SaveChanges();
                                var localId = Interlocked.Increment(ref id);
                                if(localId % 1000 == 0) {
                                    Console.WriteLine("created {0} users ", localId);
                                }
                            }
                        }
                    } catch {
                        Console.WriteLine("died after {0} ops", id);
                        throw;
                    }
                }));
            }
            Task.WaitAny(workers.ToArray());
        }

        [Test]
        public void Get_User_activity() {
            var connectionBuilder = new ConnectionBuilder(KeyspaceName, Server, true, 0, 200);
            using(var db = new CassandraContext(connectionBuilder)) {
                var userId = 848769;
                var key = new BigInteger(userId);
                var activityFamily = db.GetSuperColumnFamily("user_activity");
                var t = Stopwatch.StartNew();
                var userActivity = activityFamily
                    .Get(key)
                    .ReverseColumns()
                    .StartWithColumn(DateTime.UtcNow.AddDays(100))
                    .TakeColumns(20)
                    .FirstOrDefault()
                    .ToArray();
                t.Stop();
                var columnCount = 0;
                //var columnCount = activityFamily
                //    .Get(key)
                //    .CountColumns();
                //var rowCount = 0;
                var rowCount = activityFamily.Get().Count();
                Console.WriteLine("User {0} with {1} activity events ({2} users total)", userId, columnCount, rowCount);
                Console.WriteLine("Querytime: {0:0.00}ms", t.Elapsed.TotalMilliseconds);
                foreach(dynamic activity in userActivity) {
                    var dateTime = (DateTime)activity.ColumnName;
                    Console.WriteLine(String.Format("{0:s} : {1} - {2}",
                                                    dateTime,
                                                    activity.Type,
                                                    activity.PageId
                                          ));
                }
            }
        }

        [Test]
        public void Create_and_read_some_activity_with_supercolumn() {
            const string userActivityCFName = "user_activity_super";
            var r = new Random();
            var key = new BigInteger(r.Next(10));
            var connectionBuilder = new ConnectionBuilder(KeyspaceName, Server);
            using(var db = new CassandraContext(connectionBuilder)) {
                var keyspace = db.Keyspace;
                if(!keyspace.ColumnFamilyExists(userActivityCFName)) {
                    Console.WriteLine("creating user activity column family");
                    keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
                        FamilyName = userActivityCFName,
                        FamilyType = ColumnType.Super,
                        KeyValueType = CassandraType.IntegerType,
                        SuperColumnNameType = CassandraType.DateType,
                        ColumnNameType = CassandraType.UTF8Type,
                        DefaultColumnValueType = CassandraType.UTF8Type
                    });
                }
                Console.WriteLine("creating user record {0}", key);
                var activityFamily = db.GetSuperColumnFamily(userActivityCFName);
                var userActivity = activityFamily.Get(key).FirstOrDefault();
                var activityDate = DateTime.UtcNow.AddDays(-r.Next(10));

                // add some activity
                for(var i = 0; i < 100; i++) {
                    dynamic activity = userActivity.CreateSuperColumn();
                    activity.Type = "view";
                    activity.PageId = i;
                    userActivity[activityDate] = activity;
                    activityDate = activityDate.AddMinutes(r.Next(60));
                    db.SaveChanges(userActivity);
                }
            }
            using(var db = new CassandraContext(connectionBuilder)) {
                Console.WriteLine("get user record {0}", key);
                var activityFamily = db.GetSuperColumnFamily(userActivityCFName);
                var userActivity = activityFamily
                    .Get(key)
                    .ReverseColumns()
                    .StartWithColumn(DateTime.UtcNow)
                    .TakeColumns(10)
                    .FirstOrDefault();
                foreach(dynamic activity in userActivity) {
                    var dateTime = (DateTime)activity.ColumnName;

                    Console.WriteLine(String.Format("{0:T} : {1} - {2})",
                        dateTime,
                        activity.Type,
                        activity.PageId
                    ));
                }
            }
        }
        [Test]
        public void Create_and_read_some_activity_with_composite_key() {
            const string userActivityCFName = "user_activity_composite";
            var r = new Random();
            var userId = new BigInteger(r.Next(10));
            var connectionBuilder = new ConnectionBuilder(KeyspaceName, Server);
            using(var db = new CassandraContext(connectionBuilder)) {
                var keyspace = db.Keyspace;
                // create super column family using API
                if(!keyspace.ColumnFamilyExists(userActivityCFName)) {
                    Console.WriteLine("creating user activity column family");
                    keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
                        FamilyName = userActivityCFName,
                        FamilyType = ColumnType.Standard,
                        KeyValueType = CassandraType.CompositeType(CassandraType.IntegerType, CassandraType.DateType),
                        ColumnNameType = CassandraType.UTF8Type,
                        DefaultColumnValueType = CassandraType.UTF8Type
                    });
                }
                Console.WriteLine("creating user record {0}", userId);
                var activityFamily = db.GetColumnFamily(userActivityCFName);
                // add some activity
                var activityDate = DateTime.UtcNow.AddDays(-r.Next(10));
                for(var i = 0; i < 100; i++) {
                    var key = new CompositeType<IntegerType, DateType>(userId, activityDate);
                    var userActivity = activityFamily.CreateRecord(key) as dynamic;
                    db.Attach(userActivity);
                    userActivity.Type = "view";
                    userActivity.PageId = i;
                    activityDate = activityDate.AddMinutes(r.Next(60));
                    db.SaveChanges(userActivity);
                }
            }
            using(var db = new CassandraContext(connectionBuilder)) {
                Console.WriteLine("get user record {0}", userId);
                var activityFamily = db.GetColumnFamily(userActivityCFName);
                var userActivity = (from activity in activityFamily
                                   where activity.Key == userId
                                   select activity)
                foreach(dynamic activity in userActivity) {
                    var dateTime = (DateTime)activity.ColumnName;

                    Console.WriteLine(String.Format("{0:T} : {1} - {2})",
                        dateTime,
                        activity.Type,
                        activity.PageId
                    ));
                }
            }
        }

        [Test]
        public void Comments_wtf() {
            using(var db = new CassandraContext(keyspace: KeyspaceName, server: Server)) {
                if(db.KeyspaceExists(KeyspaceName))
                    db.DropKeyspace(KeyspaceName);

                var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema {
                    Name = KeyspaceName,
                }, db);

                keyspace.TryCreateSelf();

                // create super column family using API
                keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
                    FamilyName = "Comments",
                    FamilyType = ColumnType.Super,
                    KeyValueType = CassandraType.AsciiType,
                    SuperColumnNameType = CassandraType.DateType,
                    ColumnNameType = CassandraType.UTF8Type,
                    DefaultColumnValueType = CassandraType.UTF8Type
                });
            }
            using(var db = new CassandraContext(keyspace: KeyspaceName, server: Server)) {
                var key = "first-blog-post";

                // get the comments family
                var commentsFamily = db.GetSuperColumnFamily("Comments");
                var postComments = commentsFamily.CreateRecord(key: key);
                db.Attach(postComments);
                var dt = new DateTime(2010, 11, 29, 5, 03, 00, DateTimeKind.Local);

                // add 5 comments
                for(int i = 0; i < 5; i++) {
                    var comment = postComments.CreateSuperColumn();
                    comment["Name"] = "Nick Berardi";
                    comment["Email"] = "nick@coderjournal.com";

                    // you can also use it as a dynamic object
                    dynamic dcomment = comment;
                    dcomment.Website = "www.coderjournal.com";
                    dcomment.Comment = "Wow fluent cassandra is really great and easy to use.";

                    var commentPostedOn = dt;
                    postComments[commentPostedOn] = comment;

                    Console.WriteLine("Comment " + (i + 1) + " Posted On " + commentPostedOn.ToLongTimeString());
                    dt = dt.AddMinutes(2);
                }

                // save the comments
                db.SaveChanges();
            }
            using(var db = new CassandraContext(keyspace: KeyspaceName, server: Server)) {
                var key = "first-blog-post";
                var lastDate = DateTime.Now;

                // get the comments family
                var commentsFamily = db.GetSuperColumnFamily("Comments");

                for(int page = 0; page < 2; page++) {
                    // lets back the date off by a millisecond so we don't get paging overlaps
                    lastDate = lastDate.AddMilliseconds(-1D);

                    Console.WriteLine("showing page " + page + " of comments starting at " + lastDate.ToLocalTime());

                    // query using API
                    var comments = commentsFamily.Get(key)
                        .ReverseColumns()
                        .StartWithColumn(lastDate)
                        .TakeColumns(3)
                        .FirstOrDefault();

                    foreach(dynamic comment in comments) {
                        var dateTime = (DateTime)comment.ColumnName;

                        Console.WriteLine(String.Format("{0:T} : {1} ({2} - {3})",
                            dateTime.ToLocalTime(),
                            comment.Name,
                            comment.Email,
                            comment.Website
                        ));

                        lastDate = dateTime;
                    }
                }
            }
        }

    }
}
