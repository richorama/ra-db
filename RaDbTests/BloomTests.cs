using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDbTests
{
    [TestClass]
    public class BloomTests
    {
        [TestMethod]
        public void BasicTest()
        {
            var filter = new BloomFilter<string>(10000);
            filter.Add("foo");
            filter.Add("bar");

            Assert.IsTrue(filter.Contains("foo"));
            Assert.IsTrue(filter.Contains("bar"));
            Assert.IsFalse(filter.Contains("baz"));

            filter.Add("baz");

            Assert.IsTrue(filter.Contains("baz"));
        }
    }
}
