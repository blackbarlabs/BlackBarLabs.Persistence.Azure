using System;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    // ReSharper disable once InconsistentNaming
    public static class AzureStorageHelpers
    {
        public static readonly DateTime MinDate = new DateTimeOffset(1601, 01, 01, 0, 0, 0, new TimeSpan()).UtcDateTime;
        public static readonly DateTime MaxDate = new DateTimeOffset(9999, 12, 31, 0, 0, 0, new TimeSpan()).UtcDateTime;

        public static readonly DateTime MinDateTime = new DateTime(1601, 01, 01, 0, 0, 0, 0);
        public static readonly DateTime MaxDateTime = new DateTime(9999, 12, 31, 0, 0, 0, 0);

        public const int MaxStringLength = 32 * 1024; // strings are double-byte encoded, utf-16

        public const int MaxByteArrayLength = 64 * 1024;

        //public const string MediaContainer = "media";

        public static readonly Guid MinGuid = Guid.Empty;

        public static readonly Guid MaxGuid = new Guid("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

        public const string RowKeySeparator = "_";  // The following are disallowed in RowKey per https://msdn.microsoft.com/library/azure/dd179338.aspx:  / \ # ? \t \n \r

        public const int DefaultPageNumber = 1;

        public const int DefaultItemsPerPage = 10;

        public static string AsRowKey(this Guid id)
        {
            return id.ToString("N");
        }

        public static Guid AsGuid(this string rowKey)
        {
            return Guid.Parse(rowKey);
        }

        public static string HashGuids(params Guid[] guids)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var guid in guids)
            {
                sb.Append(guid.AsRowKey());
            }
            return sb.ToString();
        }

        public static string AddActiveDataSelection(this string query)
        {
            var activeFilter = TableQuery.GenerateFilterConditionForInt("StorageState", QueryComparisons.Equal,
               (int)StorageState.LiveData);
            return (string.IsNullOrEmpty(query))
                ? activeFilter
                : TableQuery.CombineFilters(query, TableOperators.And, activeFilter);
        }

        public static TableQuery<TData> AddActiveDataSelection<TData>(this TableQuery<TData> query) where TData : class
        {
            return new TableQuery<TData>().Where(query.FilterString.AddActiveDataSelection());
        }

        public static bool HasValidRange(this DateTimeOffset date)
        {
            return MinDate <= date && date <= MaxDate;
        }

        public static bool HasValidRange(this DateTime date)
        {
            return MinDate <= date && date <= MaxDate;
        }

        public static bool HasValidRange(this string text)
        {
            return text == null || text.Length <= MaxStringLength;
        }

        public static bool HasValidRange(this byte[] array)
        {
            return array == null || array.Length <= MaxByteArrayLength;
        }

        public static DateTimeOffset TruncateIfOutOfRange(this DateTimeOffset date)
        {
            if (date.HasValidRange()) return date;
            return MinDate > date ? MinDate : MaxDate;
        }
        public static DateTime TruncateIfOutOfRange(this DateTime date)
        {
            if (date.HasValidRange()) return date;
            return MinDateTime > date ? MinDateTime : MaxDateTime;
        }

        public static string TruncateIfOutOfRange(this string text)
        {
            return !text.HasValidRange() ? text.Substring(0, MaxStringLength) : text;
        }

        public static byte[] TruncateIfOutOfRange(this byte[] array)
        {
            return !array.HasValidRange() ? array.Take(MaxByteArrayLength).ToArray() : array;
        }
    }
}
