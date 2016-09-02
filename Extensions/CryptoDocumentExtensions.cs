using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Identity.AzureStorageTables
{
    public static class CryptoDocumentExtensions
    {
        public static void DecryptFields(this ITableEntity document)
        {
            var cryptoProperties = document.GetType().GetProperties()
                .Where(prop => prop.GetCustomAttributes(true).Length > 0); //typeof(CryptoAttribute), true));
            foreach (var cryptoProperty in cryptoProperties)
            {
                var encryptedValue = cryptoProperty.GetValue(document);
                var decryptedValue = Decrypt(encryptedValue);
                cryptoProperty.SetValue(document, decryptedValue);
            }
        }

        //public static void DecryptBytes(this Blob document)
        //{
        //    var cryptoProperties = document.GetType().GetProperties()
        //        .Where(prop => prop.GetCustomAttributes(true).Length > 0); //typeof(CryptoAttribute), true));
        //    foreach (var cryptoProperty in cryptoProperties)
        //    {
        //        var encryptedValue = cryptoProperty.GetValue(document);
        //        var decryptedValue = Decrypt(encryptedValue);
        //        cryptoProperty.SetValue(document, decryptedValue);
        //    }
        //}

        public static void EncryptFields(this ITableEntity document)
        {
            var cryptoProperties = document.GetType().GetProperties()
                .Where(prop => prop.GetCustomAttributes(true).Length > 0); //typeof(CryptoAttribute), true));
            foreach(var cryptoProperty in cryptoProperties)
            {
                var decryptedValue = cryptoProperty.GetValue(document);
                var encryptedValue = Encrypt(decryptedValue);
                cryptoProperty.SetValue(document, encryptedValue);
            }
        }

        private static object Encrypt(object decryptedValue)
        {
            throw new NotImplementedException();
        }

        private static object Decrypt(object encryptedValue)
        {
            // TODO: Load cyphers, etc
            throw new NotImplementedException();
        }
    }
}
