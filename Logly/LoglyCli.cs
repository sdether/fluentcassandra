using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using FluentCassandra;
using FluentCassandra.Connections;

namespace Logly {
    internal class LoglyCli {
        public static readonly Server Server = new Server(ConfigurationManager.AppSettings["TestServer"]);
        public static readonly string KeyspaceName = "mt_log";

        private static void Main(string[] args) {
            var connectionBuilder = new ConnectionBuilder(KeyspaceName, Server, true, 0, 200);
            if(args.Any()) {
                var userId = int.Parse(args[0]);
                var queries = new Queue<double>();
                while(true) {
                    using(var db = new CassandraContext(connectionBuilder)) {
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
                        queries.Enqueue(t.Elapsed.TotalMilliseconds);
                        var columnCount = 0;
                        //var columnCount = activityFamily
                        //    .Get(key)
                        //    .CountColumns();
                        //var rowCount = 0;
                        var rowCount = activityFamily.Get().Count();
                        Console.Clear();
                        Console.WriteLine("User {0} with {1} activity events ({2} users total)", userId, columnCount, rowCount);
                        Console.WriteLine("Querytime: {0:0.00}ms ({1:0.00}ms)", t.Elapsed.TotalMilliseconds, queries.Average());
                        foreach(dynamic activity in userActivity) {
                            var dateTime = (DateTime)activity.ColumnName;
                            Console.WriteLine(String.Format("{0:s} : {1} - {2}",
                                dateTime,
                                activity.Type,
                                activity.PageId
                            ));
                        }
                        if(queries.Count > 10) {
                            queries.Dequeue();
                        }
                        Thread.Sleep(1000);
                    }
                }
            } else {
                var r = new Random();
                var c = 0;
                var total = 0;
                var n = 5000;
                var workers = 100;
                var users = 10000000;
                var userFactor = users / workers;
                var padlock = new object();
                var userset = new HashSet<int>();
                for(var i = 0; i < 10000; i++) {
                    userset.Add(r.Next(users));
                }
                Console.WriteLine("users: {0}", string.Join(",", userset.Take(100).ToArray()));
                var t = Stopwatch.StartNew();
                var userIds = new HashSet<int>();
                for(var i = 0; i < workers; i++) {
                    var worker = i;
                    new Thread(() => {
                        Console.WriteLine("worker {0} started", worker);
                        var activityDate = DateTime.UtcNow;
                        while(true) {
                            using(var db = new CassandraContext(connectionBuilder)) {
                                for(var j = 0; j < 1000; j++) {
                                    var userId = userset.ElementAt(r.Next(userset.Count()));
                                    var key = new BigInteger(userId);
                                    var activityFamily = db.GetSuperColumnFamily("user_activity");
                                    var userActivity = activityFamily
                                        .Get(key)
                                        .TakeColumns(1)
                                        .FirstOrDefault();
                                    dynamic activity = userActivity.CreateSuperColumn();
                                    activity.Type = "view";
                                    activity.PageId = r.Next(1000);
                                    userActivity[activityDate] = activity;
                                    activityDate = activityDate.AddSeconds(r.Next(60));
                                    db.SaveChanges(userActivity);
                                    lock(padlock) {
                                        userIds.Add(userId);
                                        c++;
                                        total++;
                                        if(c % n == 0) {
                                            c = 0;
                                            Console.WriteLine("total events {0}: logged {1} events at {2}events/second - {3}",
                                                total,
                                                n,
                                                n / t.Elapsed.TotalSeconds,
                                                userIds.Count
                                            );
                                            t = Stopwatch.StartNew();
                                        }
                                    }
                                }
                            }
                        }
                    }).Start();
                }
                Console.Read();
            }
        }
    }
}
