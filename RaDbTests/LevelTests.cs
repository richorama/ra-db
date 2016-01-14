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
    public class LevelTests
    {
        [TestMethod]
        public void TestLevel()
        {
            using (var log = new Log(new MemoryStream()))
            {
                for (var i = 0; i < 1000; i++)
                {
                    log.Set($"key{i}", $"value{i}"); 
                }
                log.Del("key88");

                using (var level = Level.Build(log, new MemoryStream()))
                {
                    Assert.AreEqual("value100", level.Get("key100"));
                    Assert.AreEqual("value999", level.Get("key999"));
                    Assert.IsNull(level.Get("random key name"));
                    Assert.IsNull(level.Get("key88"));
                }
            }
        }
    }
}
