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

            using (var db = new Database<TestEntry>("testdb"))
            {
                db.Set("foo", new TestEntry("bar"));
                Assert.AreEqual("bar", db.Get("foo").Value);
            }

            using (var db = new Database<TestEntry>("testdb"))
            {
                Assert.AreEqual("bar", db.Get("foo").Value);
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }


        [TestMethod]
        public void TestDatabaseWithLotsOfKeys()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<TestEntry>("testdb", false))
            {
                for (var i = 0; i < 100000; i++)
                {
                    db.Set(i.ToString(), new TestEntry(Guid.NewGuid().ToString()));
                }

                Assert.IsNotNull(db.Get("5000"));
                Assert.IsNull(db.Get("xxxx"));

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
            }

            using (var db = new Database<TestEntry>("testdb"))
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

            using (var db = new Database<TestEntry>("testdb", false))
            {
                for (var i = 0; i < 200000; i++)
                {
                    db.Set(i.ToString("D8"), new TestEntry(Guid.NewGuid().ToString()));
                }

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
                db.Del("00000101");

                var results = db.Between("00000100", "00000110").OrderBy(x => x.Key).ToArray();
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

            using (var db = new Database<TestEntry>("testdb"))
            {
                for (var i = 0; i < 150; i++)
                {
                    db.Set(i.ToString("D8"), new TestEntry(Guid.NewGuid().ToString()));
                }

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
                db.Del("00000101");

                var results = db.Between("00000100", "00000110").OrderBy(x => x.Key).ToArray();
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

            using (var db = new Database<TestEntry>("testdb"))
            {
                var records = new KeyValue<TestEntry>[] {
                    new KeyValue<TestEntry>("one",new TestEntry("1")),
                    new KeyValue<TestEntry>("two",new TestEntry("2")),
                    new KeyValue<TestEntry>("three",new TestEntry("3"))
                };

                db.SetMulti(records);

                Assert.AreEqual("1", db.Get("one").Value);
                Assert.AreEqual("2", db.Get("two").Value);
                Assert.AreEqual("3", db.Get("three").Value);
            }
            using (var db = new Database<TestEntry>("testdb"))
            {
                Assert.AreEqual("1", db.Get("one").Value);
                Assert.AreEqual("2", db.Get("two").Value);
                Assert.AreEqual("3", db.Get("three").Value);

                db.Del("one", "two", "three");

                Assert.IsNull(db.Get("one"));
                Assert.IsNull(db.Get("two"));
                Assert.IsNull(db.Get("three"));

            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }


        [TestMethod]
        public void WriteNulls()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database<TestClass>("testdb"))
            {
                db.Set("foo", null);

                Assert.IsNull(db.Get("foo"));
            }

            using (var db = new Database<TestClass>("testdb"))
            {
                Assert.IsNull(db.Get("foo"));
            }
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
