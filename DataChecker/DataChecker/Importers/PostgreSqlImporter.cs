using System;
using System.Data;
using System.Data.Odbc;

namespace DataCheckerProj.Importers
{
    /// <summary>
    /// Class used to override aspects of SqlQueryBuilder specific to writing PostgreSql queries.
    /// </summary>
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

        /// <summary>
        /// Verifies that constructor arguments follow an expected format i.e. the connection is of the correct type. 
        /// </summary>
        /// <param name="importConnection">The database connection needed to connect to and therefore read data from the data source.</param>
        /// <param name="importQuery">The SQL query to be used to read data from the database.</param>
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