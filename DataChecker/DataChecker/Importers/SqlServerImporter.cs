using System.Data;

namespace DataCheckerProj.Importers
{

    public class SqlServerImporter : SqlDataImporter
    {
        #region properties

        #endregion

        #region constructors

        public SqlServerImporter() : base()
        {
            // used for mocking
        }

        public SqlServerImporter(IDbConnection importConnection, string importQuery) : base(importConnection, importQuery)
        {
            // work done in base
        }

        #endregion

        #region methods

        protected override void ValidateConstructorArguments(IDbConnection importConnection, string importQuery)
        {
            base.ValidateConstructorArguments(importConnection, importQuery);

            if (importConnection.GetType() != typeof(SqlConnection))
            {
                if (importConnection.GetType().ToString() != "Castle.Proxies.IDbConnectionProxy") // Rough solution to unit testing problem: dont error when Mock is passed
                {
                    string msg = "Provided importConnection must be of type SqlConnection. Actual type is " + importConnection.GetType();
                    throw new ArgumentException(msg);
                }
            }
        }

        #endregion
    }
}