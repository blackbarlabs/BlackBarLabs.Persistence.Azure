using System;
using System.Collections.Generic;
using System.Linq;
<<<<<<< HEAD
using BlackBarLabs.Collections.Generic;
=======
using BlackBarLabs.Linq;
>>>>>>> 3a08f7fb7a35cf33b03b6b8ce8bea9a56930723b

namespace BlackBarLabs.Persistence.Azure.Extensions
{
    public static class ByteArrayExtensions
    {
        public static TResult AddId<TResult>(this byte[] byteArray, Guid distributorId, Func<byte[], TResult> success)
        {
            return success(byteArray.ToGuidsFromByteArray()
                .Append(distributorId)
                .ToByteArrayOfGuids());
        }

        public static TResult ContainsId<TResult>(this byte[] byteArray, Guid id, Func<TResult> found, Func<TResult> notFound)
        {
            var ids = byteArray.ToGuidsFromByteArray();
            var foundId = ids.Contains(id);
            return foundId ? found() : notFound();
        }

        public static TResult GetAllIds<TResult>(this byte[] byteArray, Func<Guid[], TResult> ids)
        {
            return ids(byteArray.ToGuidsFromByteArray());
        }

        public static TResult RemoveId<TResult>(this byte[] byteArray, Guid id, Func<byte[], TResult> success)
        {
            var guids = byteArray.ToGuidsFromByteArray().Where(currentId => currentId != id);
            return success(guids.ToByteArrayOfGuids());
        }


    }
}
