using BlackBarLabs.Persistence.Azure.Attributes;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.Documents
{
    [Serializable]
    [DataContract]
    [StorageResource(typeof(RemainderKeyGenerator), typeof(HexadecimalRangeKeyGenerator))]
    public class LookupDocument : TableEntity
    {
        [IgnoreDataMember]
        [IgnoreProperty]
        public Guid Id { get { return Guid.Parse(this.RowKey); } }

        public Guid Lookup { get; set; }
    }
}
