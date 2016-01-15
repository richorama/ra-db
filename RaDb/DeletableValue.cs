using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public class DeletableValue
    {

        public static DeletableValue FromValue(string value)
        {
            return new DeletableValue
            {
                IsDeleted = false,
                Value = value
            };
        }

        public static DeletableValue FromDelete()
        {
            return new DeletableValue
            {
                IsDeleted = true,
                Value = null
            };
        }

        public string Value { get; set; }
        public bool IsDeleted { get; set; }
    }
}
