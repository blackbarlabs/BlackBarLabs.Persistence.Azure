using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure.Attributes
{
    public struct StringKey
    {
        public string Equal { get; set; }
        public string NotEqual { get; set; }
        public string GreaterThan { get; set; }
        public string GreaterThanOrEqual { get; set; }
        public string LessThan { get; set; }
        public string LessThanOrEqual { get; set; }
    }

    public interface DocumentKeyGenerator
    {
        IEnumerable<StringKey> GetPartitionKeys();
        IEnumerable<StringKey> GetRowKeys();
    }

    public class EmptyKeyGenerator : DocumentKeyGenerator
    {
        public virtual IEnumerable<StringKey> GetPartitionKeys() => new StringKey[] { };
        public virtual IEnumerable<StringKey> GetRowKeys() => new StringKey[] { };
    }

    public class StandardKeyGenerator : EmptyKeyGenerator
    {
        //private const int
        public override IEnumerable<StringKey> GetPartitionKeys()
        {
            int absoluteBound = Math.Abs(KeyExtensions.PartitionKeyRemainder) - 1;
            int lowerBound = -absoluteBound;
            int count = absoluteBound * 2 + 1; // include zero value
            return Enumerable.Range(lowerBound, count)  // i.e. (-12, 25)
                .Select(
                    num => num.ToString())
                .Select(
                    partitionKey =>
                    {
                        return new StringKey
                        {
                            Equal = partitionKey,
                        };
                    });
        }
    }

    public class StorageResourceAttribute : Attribute
    {
        public StorageResourceAttribute(Type documentKeyGenerator)
        {
            GetDocumentKeyGenerator = () => (DocumentKeyGenerator)Activator.CreateInstance(documentKeyGenerator);
        }

        public Func<DocumentKeyGenerator> GetDocumentKeyGenerator { get; set; }
    }
}
