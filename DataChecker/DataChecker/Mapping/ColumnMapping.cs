using System.Collections.Generic;

namespace DataCheckerProj.Mapping
{
    /// <summary>
    /// Class used to store column mapping information (schema mapping).
    /// </summary>
    public class ColumnMapping
    {
        #region properties
        public int MappingID { get; set; }
        public string SourceColumnName { get; set; }
        public string SourceColumnType { get; set; }
        public string DestinationColumnName { get; set; }
        public string DestinationColumnType { get; set; }
        public bool IsIdentityColumn { get; set; }              // Important. Used to identify mappings for a column that uniquelly identifies a data element (record)
        public bool IsOrderByColumn { get; set; }               // Used to identify mappings for the single column that should be used for ordering results and segmenting samples
        public List<string> DecommissionedClasses { get; set; } // Values for this column that constitute a class that should no longer be sampled
        #endregion

        /// <summary>
        /// Construct a ColumnMapping object.
        /// </summary>
        /// <param name="id">The integer that uniquely identifies the ColumnMapping</param>
        /// <param name="srcCol">The name of the source column being mapped to another column</param>
        /// <param name="srcType">The type of the source column being mapped to another column</param>
        /// <param name="dstCol">The name of the destination column that a source column is mapped to</param>
        /// <param name="dstType">The type of the destination column that a source column is mapped to</param>
        /// <param name="isIdentity">
        ///     Whether or not the column mapping is for a column that, 
        ///     at least partially, uniquely identifies a data element (record)
        /// </param>
        /// <param name="IsOrderBy">The single column that should be used for ordering results and segmenting samples</param>

        public ColumnMapping(int id, string srcCol, string srcType, string dstCol, string dstType, bool isIdentity,bool IsOrderBy)
        {
            this.MappingID = id;
            this.SourceColumnName = srcCol;
            this.SourceColumnType = srcType;
            this.DestinationColumnName = dstCol;
            this.DestinationColumnType = dstType;
            this.IsIdentityColumn = isIdentity;
            this.IsOrderByColumn = IsOrderBy;
            this.DecommissionedClasses = new List<string>();
        }
    }
}