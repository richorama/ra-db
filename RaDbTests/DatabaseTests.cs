using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaDb;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDbTests
{
    [TestClass]
    public class DatabaseTests 
    {
        [TestMethod]
        public void SimpleDatabaseTest()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database("testdb"))
            {
                db.Set("foo", "bar");
                Assert.AreEqual("bar", db.Get("foo"));
            }

            using (var db = new Database("testdb"))
            {
                Assert.AreEqual("bar", db.Get("foo"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }


        [TestMethod]
        public void TestDatabaseWithLotsOfKeys()
        {
            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);

            using (var db = new Database("testdb"))
            {
                for (var i = 0; i < 100000; i++)
                {
                    db.Set(i.ToString(), Guid.NewGuid().ToString());
                }

                Assert.IsNotNull(db.Get("5000"));
                Assert.IsNull(db.Get("xxxx"));

                Console.WriteLine($"levels : {db.CurrentLevelNumber}");
            }

            using (var db = new Database("testdb"))
            {
                Assert.IsNotNull(db.Get("5000"));
                Assert.IsNull(db.Get("xxxx"));
            }

            if (Directory.Exists("testdb")) Directory.Delete("testdb", true);
        }

    }
}
