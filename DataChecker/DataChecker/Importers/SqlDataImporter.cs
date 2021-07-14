using System.Data;
using System;

namespace DataCheckerProj.Importers
{
    public abstract class SqlDataImporter : IDisposable
    {

        #region properties

        protected IDbConnection DbConnection;   // Maintain this connection, made an interface type for testability
        protected IDataReader DataReader;       // Used to import data via the DbConnection
        protected DataTable DataSegment;        // Stores the last segment/sample/chunk of data that has been imported

        protected bool Disposed = false;
        #endregion

        #region constructors

        public SqlDataImporter()
        {
            DataSegment = null;
        }

        protected SqlDataImporter(IDbConnection importConnection, string importQuery)
        {
            ValidateConstructorArguments(importConnection, importQuery); // throws exception if invalid

            this.DbConnection = importConnection;

            InitialiseDataReader(importQuery);
            InitialiseDataSegment();
        }

        #endregion

        #region methods

        protected virtual void ValidateConstructorArguments(IDbConnection importConnection, string importQuery)
        {
            if (String.IsNullOrEmpty(importQuery))
            {
                string msg = "Provided importQuery has null or empty value. A query must be provided for importing data.";
                throw new ArgumentException(msg);
            }
            else if (String.IsNullOrEmpty(importConnection.ConnectionString))
            {
                string msg = "Provided DbConnection has null or empty connection string.";
                throw new ArgumentException(msg);
            }
        }

        private void InitialiseDataReader(string importQuery)
        {
            if (this.DbConnection == null)
                throw new ArgumentException("DbConnection must be instantiated before calling InitialiseDataReader(importQuery).");

            if (this.DbConnection.State != ConnectionState.Open)
                this.DbConnection.Open();

            using (IDbCommand command = this.DbConnection.CreateCommand())
            {
                command.CommandTimeout = 1200;
                command.CommandText = importQuery;

                this.DataReader = command.ExecuteReader();
            }
        }

        private void InitialiseDataSegment()
        {
            if (this.DataReader == null)
            {
                throw new ArgumentException("SqlDataImporter.DataReader is null. Make sure the reader is initialised before the data segment.");
            }

            /* Get table definition dynamically */
            string colName = "";
            Type colType = null;
            DataTable dt = new DataTable();
            dt.Clear();

            for (int i = 0; i < this.DataReader.FieldCount; i++)
            {
                colName = this.DataReader.GetName(i);
                colType = this.DataReader.GetFieldType(i);

                dt.Columns.Add(colName, colType);
            }

            dt.AcceptChanges();

            this.DataSegment = dt; // finalise
        }

        public virtual DataTable NextDataSegment(int rowLimit)
        {
            if (this.DbConnection.State != ConnectionState.Open)
                this.DbConnection.Open();

            DataTable nextDataSegment = this.DataSegment.Clone(); // Clone to get column defintions
            object[] colArray = new object[this.DataReader.FieldCount];
            int rowCount = 0;
            while (rowCount < rowLimit && this.DataReader.Read())
            {
                for (int i = 0; i < this.DataReader.FieldCount; i++) // build a new data row
                {
                    colArray[i] = this.DataReader[i];
                }

                nextDataSegment.LoadDataRow(colArray, true); // add new data row to data segment
                rowCount++;
            }

            this.DataSegment = nextDataSegment; // transaction complete, this segment is now the current segment

            return this.DataSegment;
        }

        public void Dispose()
        {
            if (this.DataReader != null)
                this.DataReader.Close();

            if (this.DbConnection != null)
                this.DbConnection.Close();

            this.DataReader = null;
            this.DbConnection = null;
        }

        #endregion
    }
}