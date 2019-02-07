using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public static class EDMExtensions
    {

        public const string NullGuidKey = "a4a347f8-4ef7-444b-b1fa-c010cd475fd2";

        public static object ToObjectFromEdmTypeByteArray(this EdmType type, byte[] values)
        {
            switch (type)
            {
                case EdmType.Binary:
                    {
                        return values;
                    }
                case EdmType.Boolean:
                    {
                        if (!values.Any())
                            return default(bool?);
                        return values[0] != 0;
                    }
                case EdmType.DateTime:
                    {
                        if (!values.Any())
                            return default(DateTime?);
                        var ticks = BitConverter.ToInt64(values, 0);
                        return new DateTime(ticks);
                    }
                case EdmType.Double:
                    {
                        if (!values.Any())
                            return default(double?);
                        var value = BitConverter.ToDouble(values, 0);
                        return value;
                    }
                case EdmType.Guid:
                    {
                        if (!values.Any())
                            return default(Guid?);
                        var value = new Guid(values);
                        var nullGuidKey = new Guid(NullGuidKey);
                        if (value == nullGuidKey)
                            return null;
                        return value;
                    }
                case EdmType.Int32:
                    {
                        if (!values.Any())
                            return default(int?);
                        var value = BitConverter.ToInt32(values, 0);
                        return value;
                    }
                case EdmType.Int64:
                    {
                        if (!values.Any())
                            return default(long?);
                        var value = BitConverter.ToInt64(values, 0);
                        return value;
                    }
                case EdmType.String:
                    {
                        if (!values.Any())
                            return default(string);

                        var markerByte = values.First();
                        var textBytes = values.Skip(1).ToArray();
                        if (1 == markerByte)
                            return null;
                        if (2 == markerByte)
                            return string.Empty;

                        return Encoding.UTF8.GetString(textBytes);
                    }
            }
            throw new Exception($"Unrecognized EdmType {type}");
        }

    }
}
