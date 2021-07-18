using System.Collections.Generic;

namespace DataCheckerProj.Mapping
{
    public class ColumnMapping
    {
        #region properties
        public int MappingID { get; set; }
        public string SourceColumnName { get; set; }
        public string SourceColumnType { get; set; }
        public string DestinationColumnName { get; set; }
        public string DestinationColumnType { get; set; }
        public bool IsIdentityColumn { get; set; }              // important. Used to identify mappings for a column that uniquelly identifies a data element (record)
        public List<string> DecommissionedClasses { get; set; } // values for this column that constitute a class that should no longer be sampled
        #endregion

        public ColumnMapping(int id, string srcCol, string srcType, string dstCol, string dstType, bool isIdentity)
        {
            this.MappingID = id;
            this.SourceColumnName = srcCol;
            this.SourceColumnType = srcType;
            this.DestinationColumnName = dstCol;
            this.DestinationColumnType = dstType;
            this.IsIdentityColumn = isIdentity;
            this.DecommissionedClasses = new List<string>();
        }
    }
}
