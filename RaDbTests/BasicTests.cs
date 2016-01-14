using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaDb;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

namespace RaDbTests
{
    [TestClass]
    public class BasicTests
    {
        [TestMethod]
        public async Task BasicWriteTest()
        {
            if (File.Exists("test.db")) File.Delete("test.db");

            using (var db = new Log("test.db"))
            {
                await db.SetAsync("foo", "bar");
                await db.SetAsync("baz", "qux");

                Assert.AreEqual("bar", db.Get("foo"));
                Assert.AreEqual("qux", db.Get("baz"));
                Assert.AreEqual(2, db.Keys.Count());
                Assert.IsTrue(db.Keys.Contains("foo"));
                Assert.IsTrue(db.Keys.Contains("baz"));
            }

            using (var db = new Log("test.db"))
            {
                Assert.AreEqual("bar", db.Get("foo"));
                Assert.AreEqual("qux", db.Get("baz"));
            }

            File.Delete("test.db");

        }

        [TestMethod]
        public async Task BasicDeleteTest()
        {
            if (File.Exists("test.db")) File.Delete("test.db");

            using (var db = new Log("test.db"))
            {
                await db.SetAsync("foo", "bar");
                Assert.AreEqual("bar", db.Get("foo"));

                await db.SetAsync("foo", "baz");
                Assert.AreEqual("baz", db.Get("foo"));

                await db.DelAsync("foo");

                Assert.IsNull(db.Get("foo"));
                Assert.AreEqual(0, db.Keys.Count());

            }

            using (var db = new Log("test.db"))
            {
                Assert.AreEqual(0, db.Keys.Count());
                Assert.IsNull(db.Get("foo"));
            }

            File.Delete("test.db");

        }

        [TestMethod]
        public async Task LongValuesTest()
        {
            if (File.Exists("test.db")) File.Delete("test.db");

            var key = new string('k', 5000);
            var value = new string('v', 5000);

            using (var db = new Log("test.db"))
            {
                await db.SetAsync(key, value);
                Assert.AreEqual(value, db.Get(key));
                Assert.AreEqual(1, db.Keys.Count());
            }

            using (var db = new Log("test.db"))
            {
                Assert.AreEqual(value, db.Get(key));
                Assert.AreEqual(1, db.Keys.Count());
            }

            File.Delete("test.db");
        }

        [TestMethod]
        public async Task EventsTest()
        {
            if (File.Exists("test.db")) File.Delete("test.db");

            using (var db = new Log("test.db"))
            {
                LogEntry capturedEvent = null;
                db.LogEvent += x => 
                {
                    capturedEvent = x;
                };
                await db.SetAsync("foo", "bar");
                Assert.AreEqual("bar", db.Get("foo"));
                Assert.IsNotNull(capturedEvent);
                Assert.AreEqual("foo", capturedEvent.Key);
                Assert.AreEqual("bar", capturedEvent.Value);
                Assert.AreEqual(Operation.Write, capturedEvent.Operation);

                capturedEvent = null;
                await db.DelAsync("foo");
                Assert.IsNotNull(capturedEvent);
                Assert.AreEqual("foo", capturedEvent.Key);
                Assert.AreEqual(Operation.Delete, capturedEvent.Operation);
            }

            File.Delete("test.db");

        }

        [TestMethod]
        public void LotsOfWrites()
        {
            if (File.Exists("test.db")) File.Delete("test.db");
            
            // I'm benchmarking ~500,000 writes per second
            // which is ~25MB on disk

            var count = 500000;
            var value = Guid.NewGuid().ToString();

            using (var db = new Log("test.db"))
            {
                foreach (var key in Enumerable.Range(0, count))
                {
                    db.Set(key.ToString(), value);
                }

                Assert.AreEqual(count, db.Keys.Count());
                Assert.AreNotEqual(0, db.Size);
            }

            File.Delete("test.db");
        }


       
    }
}
