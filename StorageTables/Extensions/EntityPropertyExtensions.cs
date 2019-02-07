using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Reflection;
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
                        if (null == ep.StringValue)
                            return new byte[] { 1 };
                        if (string.Empty == ep.StringValue)
                            return new byte[] { 2 };

                        return (new byte[] { 0 }).Concat(Encoding.UTF8.GetBytes(ep.StringValue)).ToArray();
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

    }
}
