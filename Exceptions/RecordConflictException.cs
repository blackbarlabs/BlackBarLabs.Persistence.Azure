using System;

namespace BlackBarLabs.Persistence.Azure
{
    public class RecordConflictException : Exception
    {
        public RecordConflictException(string message) : base(message) 
        {
        }
    }

    public class RecordConflictException<TDocument> : RecordConflictException
    {
        public Type RecordType { get { return typeof(TDocument); } }

        public RecordConflictException(string message) : base(message) 
        {
        }
    }
}
