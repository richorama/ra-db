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

    public class TestEntry
    {
        public TestEntry()
        { }

        public TestEntry(string value)
        {
            this.Value = value;
        }
        public string Value { get; set; }
        public override string ToString()
        {
            return this.Value;
        }
    }

    [TestClass]
    public class LevelTests
    {
        [TestMethod]
        public void TestLevel()
        {
            var serializer = new Serializer<TestEntry>();
            using (var log = new Log<TestEntry>(new MemoryStream(), serializer))
            {
                if (File.Exists("temp.level")) File.Delete("temp.level");
                for (var i = 0; i < 1000; i++)
                {
                    log.Set($"key{i}", new TestEntry($"value{i}" )); 
                }
                log.Del(new string[] { "key88" });

                using (var level = Level<TestEntry>.Build(log, "temp.level", serializer))
                {
                    Assert.AreEqual("value100", level.GetValueOrDeleted("key100").Value.Value);
                    Assert.AreEqual("value999", level.GetValueOrDeleted("key999").Value.Value);
                    Assert.IsNull(level.GetValueOrDeleted("random key name"));
                    Assert.IsTrue(level.GetValueOrDeleted("key88").IsDeleted);
                }
                if (File.Exists("temp.level")) File.Delete("temp.level");
            }
        }
    }
}
