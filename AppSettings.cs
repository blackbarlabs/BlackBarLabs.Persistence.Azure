using EastFive.Web;

namespace EastFive.Azure.Persistence
{
    [ConfigAttribute]
    public static class AppSettings
    {
        [ConfigKey("Default azure storage tables connection string",
            DeploymentOverrides.Suggested,
            DeploymentSecurityConcern = false,
            PrivateRepositoryOnly = true)]
        public const string Storage = "EastFive.Azure.StorageTables.ConnectionString";
    }
}
