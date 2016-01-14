using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public enum Operation
    {
        Write,
        Delete
    }

    public class LogEntry
    {
        public static LogEntry CreateWrite(string key, string value)
        {
            return new LogEntry { Key = key, Value = value, Operation = Operation.Write };
        }

        public static LogEntry CreateDelete(string key)
        {
            return new LogEntry { Key = key, Value = "", Operation = Operation.Delete };
        }

        public string Key { get; set; }
        public string Value { get; set; }
        public Operation Operation { get; set; }
    }
}
