namespace DataCheckerProj.Helpers
{
    /// <summary>
    /// Class used to override aspects of SqlQueryBuilder specific to writing SqlServer queries.
    /// </summary>
    public class SqlServerQueryBuilder : SqlQueryBuilder
    {
        #region properties

        #endregion

        #region constructors

        public SqlServerQueryBuilder() : base()
        {
            this.IdentifierStart = "[";
            this.IdentifierEnd = "]";
        }

        #endregion

        #region methods

        #endregion
    }
}