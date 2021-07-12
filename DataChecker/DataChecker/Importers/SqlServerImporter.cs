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



        #endregion
    }
}