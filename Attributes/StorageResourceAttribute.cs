using EastFive.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

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

    public interface StringKeyGenerator
    {
        IEnumerable<StringKey> GetKeys();
    }

    public class ListKeyGenerator : StringKeyGenerator
    {
        private readonly IEnumerable<string> items;

        public ListKeyGenerator(params string[] items)
        {
            this.items = items;
        }

        public virtual IEnumerable<StringKey> GetKeys()
        {
            return items
                .NullToEmpty()
                .Select(
                    item => new StringKey
                    {
                        Equal = item,
                    });
        }
    }

    public class RemainderKeyGenerator : StringKeyGenerator
    {
        private readonly int positiveBound;

        public RemainderKeyGenerator()
            : this(KeyExtensions.PartitionKeyRemainder - 1)
        {
        }

        public RemainderKeyGenerator(int positiveBound)
        {
            this.positiveBound = positiveBound;
        }

        public virtual IEnumerable<StringKey> GetKeys()
        {
            int negativeBound = -positiveBound;
            int count = positiveBound * 2 + 1; // include zero value
            return Enumerable.Range(negativeBound, count)  // i.e. (-12, 25)
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

    public class HexadecimalRangeKeyGenerator : StringKeyGenerator
    {
        private readonly string format;
        private readonly int count;

        public HexadecimalRangeKeyGenerator()
            : this(1)
        {
        }

        public HexadecimalRangeKeyGenerator(int places)
        {
            this.format = $"x{places}";
            this.count = 0x1 << (places * 4);
        }

        public virtual IEnumerable<StringKey> GetKeys()
        {
            return Enumerable.Range(0, count)
            .Select(
                num =>
                {
                    var upper = num + 1;
                    var lowerRowKey = num.ToString(format);
                    var upperRowKey = upper.ToString(format);
                    if (upper < count)
                        return new StringKey
                        {
                            GreaterThanOrEqual = lowerRowKey,
                            LessThan = upperRowKey,
                        };
                    else
                        return new StringKey
                        {
                            GreaterThanOrEqual = lowerRowKey,
                        };
                });
        }
    }
    public class TwoPlaceHexadecimalRangeKeyGenerator : HexadecimalRangeKeyGenerator
    {
        public TwoPlaceHexadecimalRangeKeyGenerator() : base(2) { }
    }

    public class ConcatGenerator : StringKeyGenerator
    {
        private readonly StringKeyGenerator[] generators;
        public ConcatGenerator(params StringKeyGenerator[] generators)
        {
            this.generators = generators;
        }

        public IEnumerable<StringKey> GetKeys()
        {
            return generators.SelectMany(g => g.GetKeys());
        }
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class StorageResourceAttribute : Attribute
    {
        public StorageResourceAttribute(Type partitionKeyGenerator, Type rowKeyGenerator)
        {
            PartitionKeyGenerator = 
                () => (StringKeyGenerator)Activator.CreateInstance(partitionKeyGenerator);
            RowKeyGenerator = 
                () => (StringKeyGenerator)Activator.CreateInstance(rowKeyGenerator);
        }

        public Func<StringKeyGenerator> PartitionKeyGenerator { get; set; }
        public Func<StringKeyGenerator> RowKeyGenerator { get; set; }
    }
}