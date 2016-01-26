using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaDb;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Diagnostics;

namespace RaDbTests
{
    [TestClass]
    public class LogTests
    {
        Serializer<TestEntry> serializer = new Serializer<TestEntry>();

        [TestMethod]
        public void BasicWriteTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                db.Set("foo", new TestEntry("bar"));
                db.Set("baz", new TestEntry("qux"));

                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value.Value);
                Assert.AreEqual("qux", db.GetValueOrDeleted("baz").Value.Value);
                Assert.AreEqual(2, db.Keys.Count());
                Assert.IsTrue(db.Keys.Contains("foo"));
                Assert.IsTrue(db.Keys.Contains("baz"));
            }

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value.Value);
                Assert.AreEqual("qux", db.GetValueOrDeleted("baz").Value.Value);
            }

            File.Delete("test.log");

        }

   
        [TestMethod]
        public void BasicDeleteTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                db.Set("foo", new TestEntry("bar"));
                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value.Value);

                db.Set("foo", new TestEntry("baz"));
                Assert.AreEqual("baz", db.GetValueOrDeleted("foo").Value.Value);

                db.Del(new string[] { "foo" });

                Assert.IsTrue(db.GetValueOrDeleted("foo").IsDeleted);
                Assert.AreEqual(0, db.Keys.Count());
                Assert.AreEqual(1, db.DeletedKeys.Count());
                Assert.AreEqual("foo", db.DeletedKeys.First());

            }

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                Assert.AreEqual(0, db.Keys.Count());
                Assert.IsTrue(db.GetValueOrDeleted("foo").IsDeleted);
            }

            File.Delete("test.log");

        }

        [TestMethod]
        public void LongValuesTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            var key = new string('k', 5000);
            var value = new string('v', 5000);

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                db.Set(key, new TestEntry(value));
                Assert.AreEqual(value, db.GetValueOrDeleted(key).Value.Value);
                Assert.AreEqual(1, db.Keys.Count());
            }

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                Assert.AreEqual(value, db.GetValueOrDeleted(key).Value.Value);
                Assert.AreEqual(1, db.Keys.Count());
            }

            File.Delete("test.log");
        }

        [TestMethod]
        public void EventsTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                var capturedEvent = new LogEntry<TestEntry>();
                db.LogEvent += x => 
                {
                    capturedEvent = x;
                };
                db.Set("foo", new TestEntry("bar"));
                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value.Value);
                Assert.IsNotNull(capturedEvent);
                Assert.AreEqual("foo", capturedEvent.Key);
                Assert.AreEqual("bar", capturedEvent.Value.Value);
                Assert.AreEqual(Operation.Write, capturedEvent.Operation);

                capturedEvent = new LogEntry<TestEntry>();
                db.Del(new string[] { "foo" });
                Assert.IsNotNull(capturedEvent);
                Assert.AreEqual("foo", capturedEvent.Key);
                Assert.AreEqual(Operation.Delete, capturedEvent.Operation);
            }

            File.Delete("test.log");

        }

        [TestMethod]
        public void LotsOfWrites()
        {
            if (File.Exists("test.log")) File.Delete("test.log");
            
            // I'm benchmarking ~500,000 writes per second
            // which is ~25MB on disk

            var count = 500000;
            var value = Guid.NewGuid().ToString();

            
            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                var sw = Stopwatch.StartNew();
                foreach (var key in Enumerable.Range(0, count))
                {
                    db.Set(key.ToString(), new TestEntry(value));
                }
                sw.Stop();
                Console.WriteLine(sw.ElapsedMilliseconds);

                Assert.AreEqual(count, db.Keys.Count());
                Assert.AreNotEqual(0, db.Size);
            }

            File.Delete("test.log");
        }

        [TestMethod]
        public void DeleteNonExistantKeys()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                db.Del(new string[] { "foo" });
                db.Del(new string[] { "foo" });
                db.Del(new string[] { "foo" });
            }

            File.Delete("test.log");
        }

        [TestMethod]
        public void GetNonExistantKeys()
        {

            using (var db = new Log<TestEntry>(new MemoryStream(), serializer))
            {
                Assert.IsNull(db.GetValueOrDeleted("foo"));
            }

        }


        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void NullKey()
        {
            using (var db = new Log<TestEntry>(new MemoryStream(), serializer))
            {
                db.Set(null, new TestEntry("value"));
            }
        }

        [TestMethod]
        public void NullValue()
        {
            using (var db = new Log<TestEntry>(new MemoryStream(), serializer))
            {
                db.Set("key", null);
                var result = db.GetValueOrDeleted("key");
                Assert.IsNull(result.Value);
                Assert.IsFalse(result.IsDeleted);
            }
        }


        [TestMethod]
        public void TestClearLog()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                db.Set("foo", new TestEntry("bar"));
                db.Set("baz", new TestEntry("qux"));
                db.Del(new string[] { "foo" });
                db.Clear();

                Assert.AreEqual(0, db.DeletedKeys.Count());
                Assert.AreEqual(0, db.Keys.Count());
                Assert.IsNull(db.GetValueOrDeleted("baz"));
            }

            using (var db = new Log<TestEntry>("test.log", serializer))
            {
                Assert.AreEqual(0, db.DeletedKeys.Count());
                Assert.AreEqual(0, db.Keys.Count());
                Assert.IsNull(db.GetValueOrDeleted("baz"));
            }

            File.Delete("test.log");

        }
    }
}
