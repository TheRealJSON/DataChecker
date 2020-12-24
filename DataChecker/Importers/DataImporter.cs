using System.Data;
using System;

namespace DataCheckerProj.Importers
{
    public abstract class DataImporter : IDisposable
    {
       
        #region properties

        public string ConnectionString { get; set; }
        protected string LogFilePath;
        public DataTable DataSegment { get; set; }

        #endregion

        #region constructors

        public DataImporter()
        {
            ConnectionString = "";
            DataSegment = new DataTable();
        }

        #endregion

        #region methods

        public abstract DataTable NextDataSegment(int rowLimit);
        public abstract void Dispose();

        #endregion 
    }
}
