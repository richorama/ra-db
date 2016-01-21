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
        [TestMethod]
        public void BasicWriteTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<string>("test.log"))
            {
                db.Set("foo", "bar");
                db.Set("baz", "qux");

                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value);
                Assert.AreEqual("qux", db.GetValueOrDeleted("baz").Value);
                Assert.AreEqual(2, db.Keys.Count());
                Assert.IsTrue(db.Keys.Contains("foo"));
                Assert.IsTrue(db.Keys.Contains("baz"));
            }

            using (var db = new Log<string>("test.log"))
            {
                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value);
                Assert.AreEqual("qux", db.GetValueOrDeleted("baz").Value);
            }

            File.Delete("test.log");

        }

   
        [TestMethod]
        public void BasicDeleteTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<string>("test.log"))
            {
                db.Set("foo", "bar");
                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value);

                db.Set("foo", "baz");
                Assert.AreEqual("baz", db.GetValueOrDeleted("foo").Value);

                db.Del("foo");

                Assert.IsTrue(db.GetValueOrDeleted("foo").IsDeleted);
                Assert.AreEqual(0, db.Keys.Count());
                Assert.AreEqual(1, db.DeletedKeys.Count());
                Assert.AreEqual("foo", db.DeletedKeys.First());

            }

            using (var db = new Log<string>("test.log"))
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

            using (var db = new Log<string>("test.log"))
            {
                db.Set(key, value);
                Assert.AreEqual(value, db.GetValueOrDeleted(key).Value);
                Assert.AreEqual(1, db.Keys.Count());
            }

            using (var db = new Log<string>("test.log"))
            {
                Assert.AreEqual(value, db.GetValueOrDeleted(key).Value);
                Assert.AreEqual(1, db.Keys.Count());
            }

            File.Delete("test.log");
        }

        [TestMethod]
        public void EventsTest()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<string>("test.log"))
            {
                var capturedEvent = new LogEntry<string>();
                db.LogEvent += x => 
                {
                    capturedEvent = x;
                };
                db.Set("foo", "bar");
                Assert.AreEqual("bar", db.GetValueOrDeleted("foo").Value);
                Assert.IsNotNull(capturedEvent);
                Assert.AreEqual("foo", capturedEvent.Key);
                Assert.AreEqual("bar", capturedEvent.Value);
                Assert.AreEqual(Operation.Write, capturedEvent.Operation);

                capturedEvent = new LogEntry<string>();
                db.Del("foo");
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

            
            using (var db = new Log<string>("test.log"))
            {
                var sw = Stopwatch.StartNew();
                foreach (var key in Enumerable.Range(0, count))
                {
                    db.Set(key.ToString(), value);
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

            using (var db = new Log<string>("test.log"))
            {
                db.Del("foo");
                db.Del("foo");
                db.Del("foo");
            }

            File.Delete("test.log");
        }

        [TestMethod]
        public void GetNonExistantKeys()
        {

            using (var db = new Log<string>(new MemoryStream()))
            {
                Assert.IsNull(db.GetValueOrDeleted("foo"));
            }

        }


        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void NullKey()
        {
            using (var db = new Log<string>(new MemoryStream()))
            {
                db.Set(null, "value");
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void NullValue()
        {
            using (var db = new Log<string>(new MemoryStream()))
            {
                db.Set("key", null);
            }
        }


        [TestMethod]
        public void TestClearLog()
        {
            if (File.Exists("test.log")) File.Delete("test.log");

            using (var db = new Log<string>("test.log"))
            {
                db.Set("foo", "bar");
                db.Set("baz", "qux");
                db.Del("foo");
                db.Clear();

                Assert.AreEqual(0, db.DeletedKeys.Count());
                Assert.AreEqual(0, db.Keys.Count());
                Assert.IsNull(db.GetValueOrDeleted("baz"));
            }

            using (var db = new Log<string>("test.log"))
            {
                Assert.AreEqual(0, db.DeletedKeys.Count());
                Assert.AreEqual(0, db.Keys.Count());
                Assert.IsNull(db.GetValueOrDeleted("baz"));
            }

            File.Delete("test.log");

        }
    }
}
