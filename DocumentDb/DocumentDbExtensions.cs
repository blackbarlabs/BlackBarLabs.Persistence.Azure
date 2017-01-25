using System;

namespace BlackBarLabs.Persistence.Azure.DocumentDb
{
    public static class DocumentDbExtensions
    {
        public static string DocumentKey(this Guid id)
        {
            return id.ToString("N");
        }
    }
}