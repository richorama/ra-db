using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public class DeletedResult
    {

        public static DeletedResult FromValue(string value)
        {
            return new DeletedResult
            {
                IsDeleted = false,
                Value = value
            };
        }

        public static DeletedResult FromDelete()
        {
            return new DeletedResult
            {
                IsDeleted = true,
                Value = null
            };
        }

        public string Value { get; set; }
        public bool IsDeleted { get; set; }
    }
}
