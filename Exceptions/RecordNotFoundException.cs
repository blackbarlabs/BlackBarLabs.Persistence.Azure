using System;

namespace BlackBarLabs.Persistence.Azure
{
    public class RecordNotFoundException : Exception
    {
    }

    public class RecordNotFoundException<TDocument> : RecordNotFoundException
    {
        public Type RecordType { get { return typeof(TDocument); } }

        public RecordNotFoundException()
        {
        }
    }

    public class InconsistentRecordException : ArgumentException
    {
        public InconsistentRecordException(string message, string paramName, Exception innerException) 
            : base(message, paramName, innerException)
        {

        }
    }

    public class RecordAlreadyExistsException<TDocument> : Exception
    {
        public Type RecordType { get { return typeof(TDocument); } }

        public RecordAlreadyExistsException()
        {
        }
    }
}
