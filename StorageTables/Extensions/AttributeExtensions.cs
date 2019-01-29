using BlackBarLabs.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence
{
    public static class AttributeExtensions
    {
        public static IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables[]>> GetPersistenceAttributes(this Type type)
        {
            var memberQuery = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var storageMembers = type
                .GetMembers(memberQuery)
                .Where(field => field.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                .Select(field => field.PairWithValue(field.GetAttributesInterface<IPersistInAzureStorageTables>()));
            return storageMembers;
        }
    }
}
