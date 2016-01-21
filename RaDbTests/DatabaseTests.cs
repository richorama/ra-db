using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaDb;
using System;
using System.IO;
using System.Linq;

namespace RaDbTests
{
    [TestClass]
    public class DatabaseTests
    {
        [TestMethod]
        public void SimpleDatabaseTest()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<string>("testdb"))
            {
                db.Set("foo", "bar");
                Assert.AreEqual("bar", db.Get("foo"));
            }

            using (var db = new Database<string>("testdb"))
            {
                Assert.AreEqual("bar", db.Get("foo"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }


        [TestMethod]
        public void TestDatabaseWithLotsOfKeys()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<string>("testdb"))
            {
                for (var i = 0; i < 100000; i++)
                {
                    db.Set(i.ToString(), Guid.NewGuid().ToString());
                }

                Assert.IsNotNull(db.Get("5000"));
                Assert.IsNull(db.Get("xxxx"));

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
            }

            using (var db = new Database<string>("testdb"))
            {
                Assert.IsNotNull(db.Get("5000"));
                Assert.IsNull(db.Get("xxxx"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }

        [TestMethod]
        public void TestDatabaseSearch()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<string>("testdb"))
            {
                for (var i = 0; i < 200000; i++)
                {
                    db.Set(i.ToString("D8"), Guid.NewGuid().ToString());
                }

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
                db.Del("00000101");

                var results = db.Search("00000100", "00000110").ToArray();
                Assert.AreEqual(9, results.Length);
                Assert.AreEqual("00000100", results[0].Key);
                Assert.IsFalse(results.Select(x => x.Key).Contains("00000101"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }

        [TestMethod]
        public void TestDatabaseSearchWithSmallKeyRange()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<string>("testdb"))
            {
                for (var i = 0; i < 150; i++)
                {
                    db.Set(i.ToString("D8"), Guid.NewGuid().ToString());
                }

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
                db.Del("00000101");

                var results = db.Search("00000100", "00000110").ToArray();
                Assert.AreEqual(9, results.Length);
                Assert.AreEqual("00000100", results[0].Key);
                Assert.IsFalse(results.Select(x => x.Key).Contains("00000101"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }




        [TestMethod]
        public void SimpleDatabaseTestWithPoco()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<TestClass>("testdb"))
            {
                db.Set("foo", new TestClass { Foo = "foo", Bar = 1337, Baz=true, Qux = Math.PI });
                Assert.IsNotNull(db.Get("foo"));
            }

            using (var db = new Database<TestClass>("testdb"))
            {
                var foo = db.Get("foo");
                Assert.AreEqual("foo", foo.Foo);
                Assert.AreEqual(1337, foo.Bar);
                Assert.IsTrue(foo.Baz);
                Assert.AreEqual(Math.PI, foo.Qux);
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }

        [TestMethod]
        public void MultiWrite()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<int>("testdb"))
            {
                var records = new KeyValue<int>[] {
                    new KeyValue<int>("one",1),
                    new KeyValue<int>("two",2),
                    new KeyValue<int>("three",3)
                };

                db.Set(records);

                Assert.AreEqual(1, db.Get("one"));
                Assert.AreEqual(2, db.Get("two"));
                Assert.AreEqual(3, db.Get("three"));
            }
            using (var db = new Database<int>("testdb"))
            {
                Assert.AreEqual(1, db.Get("one"));
                Assert.AreEqual(2, db.Get("two"));
                Assert.AreEqual(3, db.Get("three"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }


    }

    [Serializable]
    public class TestClass
    {
        public string Foo { get; set; }
        public int Bar { get; set; }
        public bool Baz { get; set; }
        public double Qux { get; set; }
    }
}
