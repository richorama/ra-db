using RaDb;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Saturation
{

    public class TestValue2
    {
        public string Value { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var rand = new Random();
            if (Directory.Exists("saturation")) Directory.Delete("saturation", true);
            using (var db = new Database<TestValue2>("saturation"))
            {
                var timer = Stopwatch.StartNew();

                for (var i = 0; i < (1000 * 1000); i++)
                {
                    var kv = i.ToString();
                    db.Set(kv, new TestValue2 { Value = kv });
                    if (i % 10000 == 0) Console.WriteLine(i);
                }

                timer.Stop();
                Console.WriteLine($"{timer.ElapsedMilliseconds}");

            }


        }
    }
}
