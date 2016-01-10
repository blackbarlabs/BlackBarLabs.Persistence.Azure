using System;
using System.Collections.Generic;
using System.Linq;

namespace BlackBarLabs.Persistence.Azure
{
    public static class AzureStorageListExtensions
    {
        public static string AsDelimited<T>(this IEnumerable<T> array)
        {
            return array == null ? string.Empty : string.Join(";", array);
        }

        public static IEnumerable<T> AsArray<T>(this string delimitedList, Func<string, T> parseFunc)
        {
            return string.IsNullOrWhiteSpace(delimitedList) ? new T[] { } : delimitedList.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries).Select(parseFunc);
        }


        private static string InsertDashesIntoStringGuids(this string delimitedList)
        {
            return delimitedList.AsArray(Guid.Parse).AsDelimited();
        }

        public static bool AttemptIdInsert(this string delimitedList, Guid id, out string result)
        {
            delimitedList = delimitedList.InsertDashesIntoStringGuids();
            var idString = id.ToString();
            if (string.IsNullOrWhiteSpace(delimitedList))
            {
                result = idString;
                return true;
            }
            if (delimitedList.Contains(idString))
            {
                result = default(string);
                return false;
            }
            result = string.Concat(delimitedList, ";", idString);
            return true;
        }

        public static bool AttemptIdRemoval(this string delimitedList, Guid id, out string result)
        {
            delimitedList = delimitedList.InsertDashesIntoStringGuids();
            var idString = id.ToString();
            if (string.IsNullOrWhiteSpace(delimitedList))
            {
                result = string.Empty;
                return false;
            }
            if (delimitedList.Contains(idString))
            {
                result = RemoveId(delimitedList, idString);
                return true;
            }
            result = delimitedList;
            return false;
        }

        public static string AddId(this string delimitedList, string id)
        {
            delimitedList = delimitedList.InsertDashesIntoStringGuids();
            return string.IsNullOrWhiteSpace(delimitedList) ? id : string.Concat(delimitedList, ";", id);
        }

        public static string RemoveId(this string delimitedList, Guid id)
        {
            return RemoveId(delimitedList, id.ToString());
        }

        public static string RemoveId(this string delimitedList, string id)
        {
            delimitedList = delimitedList.InsertDashesIntoStringGuids();
            if (string.IsNullOrWhiteSpace(delimitedList)) return string.Empty;
            var list = delimitedList.Split(';');
            var newList = list.Where(parsedId => parsedId != id).ToList();
            return newList.AsDelimited();
        }

        public static bool ContainsId(this string delimitedList, Guid id)
        {
            delimitedList = delimitedList.InsertDashesIntoStringGuids();
            return !string.IsNullOrWhiteSpace(delimitedList) && delimitedList.AsArray(Guid.Parse).Any(x => x == id);
        }
    }
}
