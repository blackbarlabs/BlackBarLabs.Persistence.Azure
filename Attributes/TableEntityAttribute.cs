using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence
{
    public class TableEntityAttribute : Attribute
    {
        public string TableName { get; set; }
    }
}
