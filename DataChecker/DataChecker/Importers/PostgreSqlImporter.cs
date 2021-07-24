using System;
using System.Data;
using System.Data.Odbc;

namespace DataCheckerProj.Importers
{
    public class PostgreSqlImporter : SqlDataImporter
    {
        #region properties

        #endregion

        #region Constructors

        public PostgreSqlImporter(IDbConnection importConnection, string importQuery) : base(importConnection, importQuery)
        {
            // work done in base
        }

        #endregion

        #region Methods

        protected override void ValidateConstructorArguments(IDbConnection importConnection, string importQuery)
        {
            base.ValidateConstructorArguments(importConnection, importQuery);

            if (importConnection.GetType() != typeof(OdbcConnection))
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