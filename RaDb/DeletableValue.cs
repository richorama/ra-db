using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public class DeletableValue<T>
    {

        public static DeletableValue<T> FromValue(T value)
        {
            return new DeletableValue<T>
            {
                IsDeleted = false,
                Value = value
            };
        }

        public static DeletableValue<T> FromDelete()
        {
            return new DeletableValue<T>
            {
                IsDeleted = true,
                Value = default(T)
            };
        }

        public T Value { get; set; }
        public bool IsDeleted { get; set; }
    }
}
