using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public class KeyValue<T>
    {
        public KeyValue()
        {

        }

        public KeyValue(string key, T value)
        {
            this.Key = key;
            this.Value = value;
        }

        public string Key { get; set; }
        public T Value { get; set; }
    }
}
