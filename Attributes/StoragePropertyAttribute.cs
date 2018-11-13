using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence
{
    public class StoragePropertyAttribute : Attribute
    {
        public string Name { get; set; }
        public bool IsRowKey { get; set; }
    }
}
