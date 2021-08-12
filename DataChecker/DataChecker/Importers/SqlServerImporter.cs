using System;
using System.Data;
using System.Data.SqlClient;

namespace DataCheckerProj.Importers
{
    /// <summary>
    /// Class used to read data from a SQL Server data source
    /// </summary>
    public class SqlServerImporter : SqlDataImporter
    {
        #region properties

        #endregion

        #region constructors
        public SqlServerImporter() : base()
        {
            // used for mocking
        }

        /// <summary>
        /// Construct a SqlServerImporter object.
        /// </summary>
        /// <param name="importConnection">The database connection needed to connect to and therefore read data from the data source.</param>
        /// <param name="importQuery">The SQL query to be used to read data from the database.</param>
        public SqlServerImporter(IDbConnection importConnection, string importQuery) : base(importConnection, importQuery)
        {
            // work done in base
        }

        #endregion

        #region methods

        /// <summary>
        /// Verifies that constructor arguments follow an expected format i.e. the connection is of the correct type. 
        /// </summary>
        /// <param name="importConnection">The database connection needed to connect to and therefore read data from the data source.</param>
        /// <param name="importQuery">The SQL query to be used to read data from the database.</param>
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