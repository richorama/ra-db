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

    public class LogEntry<T>
    {
        public static LogEntry<T> CreateWrite(string key, T value)
        {
            return new LogEntry<T> { Key = key, Value = value, Operation = Operation.Write };
        }

        public static LogEntry<T> CreateDelete(string key)
        {
            return new LogEntry<T> { Key = key, Value = default(T), Operation = Operation.Delete };
        }

        public string Key { get; set; }
        public T Value { get; set; }
        public Operation Operation { get; set; }
    }
}
