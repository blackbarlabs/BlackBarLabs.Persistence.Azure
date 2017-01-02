using System;

namespace BlackBarLabs.Persistence.Azure.SQLServer
{
    public static class Exceptions
    {
        public static bool DuplicateKeyViolation(this Exception ex)
        {
            if (ex != null && ex.InnerException != null && ex.InnerException.InnerException != null)
                return ex.InnerException.InnerException.Data["HelpLink.EvtID"].ToString() == "2627";
            return false;
        }
    }
}