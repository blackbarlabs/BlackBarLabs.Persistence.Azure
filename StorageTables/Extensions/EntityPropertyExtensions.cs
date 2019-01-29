using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Serialization;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public static class EntityPropertyExtensions
    {
        private static Dictionary<EdmType, byte> typeBytes = new Dictionary<EdmType, byte>
            {
                { EdmType.Binary, 1 },
                { EdmType.Boolean, 2 },
                { EdmType.DateTime, 3 },
                { EdmType.Double, 4 },
                { EdmType.Guid, 5 },
                { EdmType.Int32, 6 },
                { EdmType.Int64, 7},
                { EdmType.String, 8 },
            };

        public static byte[] ToByteArrayOfEntityProperties(this IEnumerable<EntityProperty> entityProperties)
        {
            return entityProperties
                .ToByteArray(
                    ep =>
                    {
                        var epBytes = ToByteArrayOfEntityProperty(ep);
                        var typeByte = typeBytes[ep.PropertyType];
                        var compositeBytes = typeByte.AsArray().Concat(epBytes).ToArray();
                        return compositeBytes;
                    });
        }

        public static byte[] ToByteArrayOfEntityProperty(this EntityProperty ep)
        {
            switch (ep.PropertyType)
            {
                case EdmType.Binary:
                    {
                        if (ep.BinaryValue.IsDefaultOrNull())
                            return new byte[] { };
                        return ep.BinaryValue;
                    }
                case EdmType.Boolean:
                    {
                        if (!ep.BooleanValue.HasValue)
                            return new byte[] { };
                        var epValue = ep.BooleanValue.Value;
                        var boolByte = epValue ? (byte)1 : (byte)0;
                        return boolByte.AsArray();
                    }
                case EdmType.DateTime:
                    {
                        if (!ep.DateTime.HasValue)
                            return new byte[] { };
                        var dtValue = ep.DateTime.Value;
                        return BitConverter.GetBytes(dtValue.Ticks);
                    }
                case EdmType.Double:
                    {
                        if (!ep.DoubleValue.HasValue)
                            return new byte[] { };
                        var epValue = ep.DoubleValue.Value;
                        return BitConverter.GetBytes(epValue);
                    }
                case EdmType.Guid:
                    {
                        if (!ep.GuidValue.HasValue)
                            return new byte[] { };
                        var epValue = ep.GuidValue.Value;
                        return epValue.ToByteArray();
                    }
                case EdmType.Int32:
                    {
                        if (!ep.Int32Value.HasValue)
                            return new byte[] { };
                        var epValue = ep.Int32Value.Value;
                        return BitConverter.GetBytes(epValue);
                    }
                case EdmType.Int64:
                    {
                        if (!ep.Int64Value.HasValue)
                            return new byte[] { };
                        var epValue = ep.Int64Value.Value;
                        return BitConverter.GetBytes(epValue);
                    }
                case EdmType.String:
                    {
                        if (ep.StringValue.IsDefaultOrNull())
                            return new byte[] { };
                        return Encoding.UTF8.GetBytes(ep.StringValue);
                    }
            }
            throw new Exception($"Unrecognized EdmType {ep.PropertyType}");
        }

        public static object[] FromEdmTypedByteArray(this byte[] binaryValue, Type type)
        {
            var typeFromByte = typeBytes
                .Select(kvp => kvp.Key.PairWithKey(kvp.Value))
                .ToDictionary();
            var arrOfObj = binaryValue
                .FromByteArray()
                .Select(
                    typeWithBytes =>
                    {
                        if (!typeWithBytes.Any())
                            return type.GetDefault();
                        var typeByte = typeWithBytes[0];
                        if(!typeFromByte.ContainsKey(typeByte))
                            return type.GetDefault();
                        var edmType = typeFromByte[typeByte];
                        var valueBytes = typeWithBytes.Skip(1).ToArray();
                        var valueObj = edmType.ToObjectFromEdmTypeByteArray(valueBytes);
                        if(null == valueObj)
                        {
                            if (type.IsClass)
                                return valueObj;
                            return type.GetDefault();
                        }
                        if (!type.IsAssignableFrom(valueObj.GetType()))
                            return type.GetDefault(); // TODO: Data corruption?
                        return valueObj;
                    })
                .ToArray();
            return arrOfObj;
        }

        private static object IRefInstance(Guid guidValue, Type type)
        {
            var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(type);
            var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
            return instance;
        }

        public static TResult ToArray<TResult>(this EntityProperty value, Type arrayType, 
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (arrayType == typeof(object))
            {
                var arrOfObj = value.BinaryValue.FromEdmTypedByteArray(arrayType);
                return onBound(arrOfObj);
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                return value.ToArray(typeof(Guid),
                    objects =>
                    {
                        var guids = (Guid[])objects;
                        var resourceType = arrayType.GenericTypeArguments.First();
                        var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);

                        var refs = guids
                            .Select(
                                guidValue =>
                                {
                                    var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                                    return instance;
                                })
                           .ToArray();
                        return onBound(refs);
                    },
                    () => throw new Exception("BindArray failed to bind to Guids?"));
            }

            if (arrayType.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                return value.ToArray(typeof(Guid?),
                    objects =>
                    {
                        var guidMaybes = (Guid?[])objects;
                        var resourceType = arrayType.GenericTypeArguments.First();
                        var instantiatableType = typeof(EastFive.RefOptional<>)
                            .MakeGenericType(resourceType);
                        
                        var refs = guidMaybes
                            .Select(
                                guidMaybe =>
                                {
                                    if (!guidMaybe.HasValue)
                                    {
                                        var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                                        return onBound(refOpt);
                                    }
                                    var guidValue = guidMaybe.Value;
                                    var refValue = IRefInstance(guidValue, resourceType);
                                    var instance = Activator.CreateInstance(instantiatableType, new object[] { refValue });
                                    return onBound(instance);
                                })
                           .ToArray();
                        return onBound(refs);
                    },
                    onFailedToBind);
            }


            var arrayOfObj = value.BinaryValue.FromEdmTypedByteArray(arrayType);
            var arrayOfType = Array.CreateInstance(arrayType, arrayOfObj.Length);
            foreach (var i in Enumerable.Range(0, arrayOfObj.Length))
            {
                arrayOfType.SetValue(arrayOfObj.GetValue(i), i);
            }
            return onBound(arrayOfType);

            return arrayType.IsNullable(
                nulledType =>
                {
                    if (nulledType.IsAssignableFrom(typeof(Guid)))
                    {
                        var values = value.BinaryValue.ToNullablesFromByteArray<Guid>(
                            (byteArray) => new Guid(byteArray));
                        return onBound(values);
                    }
                    if (nulledType.IsAssignableFrom(typeof(DateTime)))
                    {
                        var values = value.BinaryValue.ToNullableDateTimesFromByteArray();
                        return onBound(values);
                    }
                    throw new Exception($"Cannot serialize a nullable array of `{nulledType.FullName}`.");
                },
                () =>
                {
                    if (arrayType.IsAssignableFrom(typeof(Guid)))
                    {
                        var values = value.BinaryValue.ToGuidsFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsAssignableFrom(typeof(byte)))
                    {
                        return onBound(value.BinaryValue);
                    }
                    if (arrayType.IsAssignableFrom(typeof(bool)))
                    {
                        var boolArray = value.BinaryValue
                            .Select(b => b != 0)
                            .ToArray();
                        return onBound(boolArray);
                    }
                    if (arrayType.IsAssignableFrom(typeof(DateTime)))
                    {
                        var values = value.BinaryValue.ToDateTimesFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsAssignableFrom(typeof(double)))
                    {
                        var values = value.BinaryValue.ToDoublesFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsAssignableFrom(typeof(decimal)))
                    {
                        var values = value.BinaryValue.ToDecimalsFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsAssignableFrom(typeof(int)))
                    {
                        var values = value.BinaryValue.ToIntsFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsAssignableFrom(typeof(long)))
                    {
                        var values = value.BinaryValue.ToLongsFromByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsAssignableFrom(typeof(string)))
                    {
                        var values = value.BinaryValue.ToStringsFromUTF8ByteArray();
                        return onBound(values);
                    }
                    if (arrayType.IsEnum)
                    {
                        var values = value.BinaryValue.ToEnumsFromByteArray(arrayType);
                        return onBound(values);
                    }
                    throw new Exception($"Cannot serialize array of `{arrayType.FullName}`.");
                });

        }
    }
}
