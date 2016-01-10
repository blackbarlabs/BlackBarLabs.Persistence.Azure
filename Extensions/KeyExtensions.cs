using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Xml;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Persistence.Azure.StorageTables;

namespace BlackBarLabs.Persistence.Azure
{
    public static class KeyExtensions
    {
        //public static string Generate(this string partitionKey, Guid id)
        //{
        //    return id + partitionKey;
        //}

        public const int MaxGuidsPerProperty = 4000; //64000 / sizeof(default(Guid));

        #region PartitionKeyGeneration
        public static string GeneratePartitionKey(this string id)
        {
            return BuildPartitionKey(id);
        }

        private static int GetHashCode(string str)
        {
            unsafe
            {
                fixed (char* src = str)
                {
                    var hash1 = (5381 << 16) + 5381;
                    var hash2 = hash1;

                    // 32 bit machines. 
                    var pint = (int*)src;
                    var len = str.Length;
                    while (len > 2)
                    {
                        hash1 = ((hash1 << 5) + hash1 + (hash1 >> 27)) ^ pint[0];
                        hash2 = ((hash2 << 5) + hash2 + (hash2 >> 27)) ^ pint[1];
                        pint += 2;
                        len -= 4;
                    }

                    if (len > 0)
                    {
                        hash1 = ((hash1 << 5) + hash1 + (hash1 >> 27)) ^ pint[0];
                    }
                    return hash1 + (hash2 * 1566083941);
                }
            }
        }

        internal static string BuildPartitionKey(string rowKey)
        {
            var hashCode = GetHashCode(rowKey);

            return (hashCode % 13).ToString(CultureInfo.InvariantCulture);
        }
        #endregion

        public static List<string> ToListOfKeys(this string delimitedList)
        {
            if (string.IsNullOrWhiteSpace(delimitedList)) return new List<string>();
            var listOfGuids = delimitedList.Split(new[] { "|" }, StringSplitOptions.RemoveEmptyEntries).ToList();
            return listOfGuids;
        }
        public static void SetId(this ITableEntity document, Guid id)
        {
            SetId(document, id.AsRowKey());
        }

        public static void SetId(this Microsoft.WindowsAzure.Storage.Table.ITableEntity document, string id)
        {
            document.RowKey = id;
            document.PartitionKey = document.RowKey.GeneratePartitionKey();
        }
        public static string ToStringOfKeys(this List<string> keys)
        {
            if (keys == null) return string.Empty;
            return string.Join("|", keys);
        }

        public static string ToStringFromListOfGuids(this List<Guid> guids)
        {
            if (guids == null) return string.Empty;
            return string.Join("|", guids);
        }

        public static List<Guid> ToListOfGuidsFromString(this string delimitedList)
        {
            if (string.IsNullOrWhiteSpace(delimitedList)) return new List<Guid>();
            var listOfGuids = delimitedList.Split(new[] { "|" }, StringSplitOptions.RemoveEmptyEntries).Select(Guid.Parse).ToList();
            return listOfGuids;
        }

        #region ByteArray
        
        public static int [] ToIntsFromByteArray(this byte[] byteArrayOfInts)
        {
            if(byteArrayOfInts == null)
                return new int[] { };

            var intStorageLength = sizeof(int);
            return Enumerable.Range(0, byteArrayOfInts.Length/ intStorageLength)
                .Select((index) => BitConverter.ToInt32(byteArrayOfInts, index * intStorageLength))
                .ToArray();
        }
        
        public static byte[] ToByteArrayOfInts(this IEnumerable<int> ints)
        {
            return ints.SelectMany(i => BitConverter.GetBytes(i)).ToArray();
        }

        public static long[] ToLongsFromByteArray(this byte[] byteArrayOfLongs)
        {
            if (byteArrayOfLongs == null)
                return new long[] { };

            var longStorageLength = sizeof(long);
            return Enumerable.Range(0, byteArrayOfLongs.Length / longStorageLength)
                .Select((index) => BitConverter.ToInt64(byteArrayOfLongs, index * longStorageLength))
                .ToArray();
        }

        public static byte[] ToByteArrayOfLongs(this IEnumerable<long> longs)
        {
            return longs.SelectMany(i => BitConverter.GetBytes(i)).ToArray();
        }

        public static Guid[] ToGuidsFromByteArray(this byte[] byteArrayOfGuids)
        {
            if (byteArrayOfGuids == null)
                return new Guid[] { };

            var guidStorageLength = Guid.NewGuid().ToByteArray().Length;
            return Enumerable.Range(0, byteArrayOfGuids.Length / guidStorageLength)
                .Select((index) => byteArrayOfGuids.Skip(index * guidStorageLength).Take(guidStorageLength).ToArray())
                .Select((byteArray) => new Guid(byteArray))
                .ToArray();
        }

        public static byte[] ToByteArrayOfGuids(this IEnumerable<Guid> guids)
        {
            return guids.SelectMany(guid => guid.ToByteArray()).ToArray();
        }
        
        public static DateTime[] ToDateTimesFromByteArray(this byte[] byteArrayOfDates)
        {
            return byteArrayOfDates
                .ToLongsFromByteArray()
                .Select(ticks => new DateTime(ticks, DateTimeKind.Utc))
                .ToArray();
        }
        
        public static byte[] ToByteArrayOfDates(this IEnumerable<DateTime> dates)
        {
            return dates.SelectMany(date => BitConverter.GetBytes(date.Ticks)).ToArray();
        }

        public static byte[] ToByteArray<TKey, TValue>(this IDictionary<TKey, TValue> obj,
            Func<TKey, byte[]> keyConverter, Func<TValue, byte[]> valueConverter)
        {
            if(default(IDictionary<TKey, TValue>) == obj)
            {
                return BitConverter.GetBytes(((int)0));
            }

            var meat = obj.Select((kvp) =>
            {
                var keyBytes = keyConverter(kvp.Key);
                var valueBytes = valueConverter(kvp.Value);
                var bytes = new byte[][]
                {
                    BitConverter.GetBytes(keyBytes.Length),
                    keyBytes,
                    BitConverter.GetBytes(valueBytes.Length),
                    valueBytes,
                };
                return bytes.SelectMany(b => b).ToArray();
            });
            return meat.SelectMany(b => b).ToArray();
        }

        public static IDictionary<TKey, TValue> FromByteArray<TKey, TValue>(this byte [] data,
            Func<byte[], TKey> keyConverter, Func<byte[], TValue> valueConverter)
        {
            var offsets = FromByteArrayOffsets(data).ToArray();
            var byteLines = offsets
                .Select(offset =>
                    data
                        .Skip(offset + sizeof(Int32))
                        .Take(BitConverter.ToInt32(data, offset))
                        .ToArray())
                .ToArray();
            var kvps = byteLines
                .SelectEvenOdd(
                    bytes => keyConverter(bytes),
                    bytes => valueConverter(bytes));
            var result = kvps
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value);
            return result;
        }

        /// <summary>
        /// Index 0 is even so starts with even (comp sci, not math)
        /// </summary>
        public static IEnumerable<KeyValuePair<TEven, TOdd>> SelectEvenOdd<TSelect, TEven, TOdd>(
            this IEnumerable<TSelect> items, Func<TSelect, TEven> evenSelect, Func<TSelect, TOdd> oddSelect)
        {
            var itemsEnumerator = items.GetEnumerator();
            while(itemsEnumerator.MoveNext())
            {
                var evenValue = evenSelect.Invoke(itemsEnumerator.Current);
                if (!itemsEnumerator.MoveNext())
                    break;
                yield return new KeyValuePair<TEven, TOdd>(evenValue, oddSelect.Invoke(itemsEnumerator.Current));
            }
        }
        private static IEnumerable<Int32> FromByteArrayOffsets(byte [] data)
        {
            if (data == null)
                yield break;

            int index = 0;
            while (index < data.Length)
            {
                yield return index;
                var offset = BitConverter.ToInt32(data, index);
                index += offset + sizeof(Int32);
            }
        }

        public static byte[] ToByteArray<TKey, TValue>(this IDictionary<TKey, TValue> obj, Func<TValue, byte []> lineConverter)
        {
            var byte1 = BitConverter.GetBytes(obj.Keys.Count);
            var meat = obj.Select((kvp) => lineConverter(kvp.Value));
            var offsets = meat.Select((piece) => piece.Length).Select(pieceLength => BitConverter.GetBytes(pieceLength)).SelectMany(b => b);
            return new byte[][]
            {
                byte1,
                offsets.ToArray(),
                meat.SelectMany(piece => piece).ToArray(),
            }.SelectMany(b => b).ToArray();
        }
        public static byte[] FromByteArray<TKey, TValue>(this IDictionary<TKey, TValue> obj, Func<TValue, byte[]> lineConverter)
        {
            var byte1 = BitConverter.GetBytes(obj.Keys.Count);
            var meat = obj.Select((kvp) => lineConverter(kvp.Value));
            var offsets = meat.Select((piece) => piece.Length).Select(pieceLength => BitConverter.GetBytes(pieceLength)).SelectMany(b => b);
            return new byte[][]
            {
                byte1,
                offsets.ToArray(),
                meat.SelectMany(piece => piece).ToArray(),
            }.SelectMany(b => b).ToArray();
        }
        
        public static Nullable<T>[] ToNullablesFromByteArray<T>(this byte[] byteArrayOfNullables, Func<byte[], T> convert, int constantSize = -1)
            where T : struct
        {
            if (byteArrayOfNullables == null)
                return new T?[] { };
            
            return byteArrayOfNullables
                .ToNullableEnumerableFromByteArray(convert, constantSize)
                .ToArray();
        }

        private static IEnumerable<Nullable<T>> ToNullableEnumerableFromByteArray<T>(this byte[] byteArrayOfNullables, Func<byte[], T> convert, int constantSize)
            where T : struct
        {
            var storageLength = BitConverter.ToInt32(byteArrayOfNullables, 0);
            var byteArrayOfNullable = new byte[storageLength];
            var index = sizeof(int);
            while (index < byteArrayOfNullables.Length)
            {
                if (byteArrayOfNullables[index] == 0)
                {
                    yield return new Nullable<T>();
                    if (constantSize > 0)
                        index += constantSize;
                } else
                {
                    Array.Copy(byteArrayOfNullables, index + 1, byteArrayOfNullable, 0, storageLength);
                    yield return convert(byteArrayOfNullable);
                    index += storageLength;
                }
                index++;
            }
        }

        public static byte[] ToByteArrayOfNullables<T>(this IEnumerable<Nullable<T>> nullables, Func<T, byte[]> convert, int constantSize = -1)
            where T : struct
        {
            int size = constantSize;
            if(size < 0)
            {
                size = convert(default(T)).Length;
            }

            var bytes = nullables
                .SelectMany(
                    (nullable) =>
                    {
                        if (nullable.HasValue)
                            return new byte[] { 1 }.Concat(convert(nullable.Value));
                        if(-1 == constantSize)
                            return new byte [] { 0 };
                        return Enumerable.Repeat((byte)0, constantSize + 1).ToArray();
                    });
            return BitConverter.GetBytes(size).Concat(bytes).ToArray();
        }
        
        public static byte[] ToByteArray(this IEnumerable<byte[]> items)
        {
            var meat = items.Select((itemBytes) =>
            {
                var bytes = new byte[][]
                {
                    BitConverter.GetBytes(itemBytes.Length),
                    itemBytes,
                };
                return bytes.SelectMany(b => b).ToArray();
            });
            return meat.SelectMany(b => b).ToArray();
        }

        public static IEnumerable<byte []> FromByteArray(this byte[] data)
        {
            var offsets = FromByteArrayOffsets(data).ToArray();
            var byteLines = offsets.Select(offset =>
                data.Skip(offset + sizeof(Int32)).
                Take(BitConverter.ToInt32(data, offset)).
                ToArray());
            return byteLines;
        }

        internal static List<Guid> GetGuidStorageString(this string storageString)
        {
            var list = GetGuidStorage(storageString);
            return list ?? new List<Guid>();
        }

        public static string SetGuidStorageString(this List<Guid> steps)
        {
            return Encode(steps);
        }
        
        public static string SetGuidStorageString(this Guid [] steps)
        {
            return SetGuidStorageString(new List<Guid>(steps));
        }

        private static List<Guid> GetGuidStorage(string storage)
        {
            return storage == null ? new List<Guid>() : Decode<List<Guid>>(storage);
        }
        #endregion

        #region Hashes


        public static Guid MD5HashGuid(this byte [] bytes, MD5 md5 = default(MD5))
        {
            if (default(MD5) == md5)
                md5 = MD5.Create();

            byte[] data = md5.ComputeHash(bytes);
            return new Guid(data);
        }

        public static Guid MD5HashGuid(this string concatination, MD5 md5 = default(MD5))
        {
            if(default(MD5) == md5)
                md5 = MD5.Create();

            byte[] data = md5.ComputeHash(Encoding.UTF8.GetBytes(concatination));
            return new Guid(data);
        }

        public static string MD5HashString(this string concatination, MD5 md5 = default(MD5))
        {
            if (default(MD5) == md5)
                md5 = MD5.Create();

            byte[] data = md5.ComputeHash(Encoding.UTF8.GetBytes(concatination));
            return Convert.ToBase64String(data);
        }

        #endregion

        #region Object Storage String
        internal static List<Guid> GetTDocumentStorageString(this string storageString)
        {
            return GetGuidStorage(storageString);
        }

        public static string SetTDocumentStorageString<TDocument>(List<TDocument> steps)
        {
            return Encode(steps);
        }

        private static List<TDocument> GetTDocumentStorage<TDocument>(string storage)
        {
            return storage == null ? new List<TDocument>() : Decode<List<TDocument>>(storage);
        }
        #endregion 


        //abstract class AtomicEntity<TKey, TDocument> : IDisposable
        //where TDocument : TableEntity, IDocument

        internal static T Decode<T>(string value)
        {
            value = value.Replace("-", "");
            if (String.IsNullOrWhiteSpace(value))
            {
                return default(T);
            }
            var reader = XmlReader.Create(new StringReader(value));
            var serializer = new DataContractSerializer(typeof(T));
            try
            {
                T result = (T)serializer.ReadObject(reader);
                return result;
            }
            catch (SerializationException)
            {
                return default(T);
            }
        }

        internal static string Encode<T>(T value)
        {
            if (EqualityComparer<T>.Default.Equals(value))
            {
                return String.Empty;
            }
            string serializedString;
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(typeof(T));
                serializer.WriteObject(memoryStream, value);
                memoryStream.Position = 0;
                serializedString = reader.ReadToEnd();
            }
            return serializedString;
        }

    }
}
