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
    class Program
    {
        static void Main(string[] args)
        {
            var rand = new Random();
            if (Directory.Exists("benchmark")) Directory.Delete("benchmark", true);
            using (var db = new Database<int>("benchmark"))
            {
                var timer = Stopwatch.StartNew();
                for (var i = 0; i < 300000; i++)
                {
                    var key = rand.Next(1, 1000).ToString();
                     // var value = db.Get(key);
                    var value = 0;
                    value++;
                    db.Set(key, value);
                }
                timer.Stop();
                Console.WriteLine($"insert time {timer.ElapsedMilliseconds}ms");
                timer.Reset();

                db.LevelUp(true);
                Console.WriteLine("compacted");

                timer.Start();
                for (var i = 0; i < 2000; i++)
                {
                    var key = rand.Next(1, 1000).ToString();
                    var value = db.Get(key);
                }
                timer.Stop();
                Console.WriteLine($"get time {timer.ElapsedMilliseconds}ms");
                timer.Reset();

                timer.Start();
                for (var i = 0; i < 2000; i++)
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
    }
}
