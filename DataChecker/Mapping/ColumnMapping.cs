using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCheckerProj.Mapping
{
    public class ColumnMapping
    {
        #region properties
        public string SourceColumnName { get; set; }
        public string SourceColumnType { get; set; }
        public string DestinationColumnName { get; set; }
        public string DestinationColumnType { get; set; }
        public bool IsIdentityColumn { get; set; }
        #endregion

        public ColumnMapping(string srcCol, string srcType, string dstCol, string dstType)
        {
            this.SourceColumnName = srcCol;
            this.SourceColumnType = srcType;
            this.DestinationColumnName = dstCol;
            this.DestinationColumnType = dstType;
        }
    }
}

