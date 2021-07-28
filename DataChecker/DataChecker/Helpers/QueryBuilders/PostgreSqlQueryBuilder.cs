namespace DataCheckerProj.Helpers
{
    /// <summary>
    /// Class used to override aspects of SqlQueryBuilder specific to writing PostgreSql queries.
    /// </summary>
    public class PostgreSqlQueryBuilder : SqlQueryBuilder
    {
        #region properties

        #endregion

        #region constructors

        public PostgreSqlQueryBuilder() : base()
        {
            this.IdentifierStart = "\"";
            this.IdentifierEnd = "\"";
            //this.IdentifierStart = "";
            //this.IdentifierEnd = "";
        }

        #endregion

        #region methods

        #endregion
    }
}