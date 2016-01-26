using RaDb;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmark
{
    public class TestValue
    {
        public int Value { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var rand = new Random();
            if (Directory.Exists("benchmark")) Directory.Delete("benchmark", true);
            using (var db = new Database<TestValue>("benchmark"))
            {
                var timer = Stopwatch.StartNew();
                for (var i = 0; i < 300000; i++)
                {
                    var key = rand.Next(1, 1000).ToString();
                    // var value = db.Get(key);
                    var value = 0;
                    value++;
                    db.Set(key, new TestValue { Value = value });
                }
                timer.Stop();
                Console.WriteLine($"insert time {timer.ElapsedMilliseconds}ms");
                timer.Reset();

                Gets(rand, db, timer);

                db.LevelUp(true);
                Console.WriteLine("compacted");

                Gets(rand, db, timer);

                timer.Start();
                for (var i = 0; i < 300000; i++)
                {
                    var key = rand.Next();
                    db.Between(key.ToString(), (key + 10).ToString()).ToArray();
                }
                timer.Stop();
                Console.WriteLine($"search time {timer.ElapsedMilliseconds}ms");
                timer.Reset();
            }
            Console.ReadKey();
        }

        private static void Gets(Random rand, Database<TestValue> db, Stopwatch timer)
        {
            timer.Start();
            for (var i = 0; i < 300000; i++)
            {
                var key = rand.Next(1, 1000).ToString();
                var value = db.Get(key);
            }
            timer.Stop();
            Console.WriteLine($"get time {timer.ElapsedMilliseconds}ms");
            timer.Reset();
        }
    }
}
