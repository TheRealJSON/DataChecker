using System.Data;
using System;

namespace DataCheckerProj.Importers
{
    /// <summary>
    /// Class used to read data from a generic SQL datasource data source
    /// </summary>
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

        /// <summary>
        /// Construct a SqlDataImporter object.
        /// </summary>
        /// <param name="importConnection">The database connection needed to connect to and therefore read data from the data source.</param>
        /// <param name="importQuery">The SQL query to be used to read data from the database.</param>
        protected SqlDataImporter(IDbConnection importConnection, string importQuery)
        {
            ValidateConstructorArguments(importConnection, importQuery); // throws exception if invalid

            this.DbConnection = importConnection;

            InitialiseDataReader(importQuery);
            InitialiseDataSegment();
        }

        #endregion

        #region methods

        /// <summary>
        /// Verifies that constructor arguments follow an expected format i.e. import query is not empty
        /// </summary>
        /// <param name="importConnection">The database connection needed to connect to and therefore read data from the data source.</param>
        /// <param name="importQuery">The SQL query to be used to read data from the database.</param>
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

        /// <summary>
        /// Initialises/bootstraps the <c>DataReader</c> property used to import data using the provided SQL query and the <c>DbConnection</c> property.  
        /// </summary>
        /// <param name="importQuery">The SQL query to be used to read data from the database.</param>
        /// <exception cref="ArgumentException">Throws exception if DBConnection is not instantiated.</exceptioon>
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

        /// <summary>
        /// Initialises/bootstraps the <c>DataTable DataSegment</c> property by using the the <c>DataReader</c> property's
        /// runtime query results to determine required table structure.
        /// </summary>
        /// <exception cref="ArgumentException">If required <c>DataReader</c> property is not instantiated.</exception>
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

        /// <summary>
        /// Reads the next series of query results from the data source into the <c>DataSegment</c> DataTable property.
        /// Data previously stored in the <c>DataSegment</c> are lost/overwritten.
        /// </summary>
        /// <param name="rowLimit">The maximum number of records/DataRows to read.</param>
        /// <return>Returns <c>DataSegment</c> property of object that has just been filled with data.</return>
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

        /// <summary>
        /// DataReader Getter.
        /// </summary>
        /// <return><c>DataReader</c> property.</return>
        public IDataReader GetDataReader()
        {
            return this.DataReader;
        }

        /// <summary>
        /// DbConnection Getter.
        /// </summary>
        /// <return><c>DbConnection</c> property.</return>
        public IDbConnection GetDbConnection()
        {
            return this.DbConnection;
        }

        /// <summary>
        /// DataSegment Getter.
        /// </summary>
        /// <return><c>DataSegment</c> property.</return>
        public virtual DataTable GetDataSegment() // used for testing/mocking
        {
            return this.DataSegment;
        }

        /// <summary>
        /// Deallocates properties, closes connections etc. 
        /// </summary>
        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);
            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// If disposing equals true, the method has been called directly
        /// or indirectly by a user's code. Managed and unmanaged resources
        /// can be disposed.
        /// If disposing equals false, the method has been called by the
        /// runtime from inside the finalizer and you should not reference
        /// other objects. Only unmanaged resources can be disposed.
        /// </summary>
        /// <param name="disposing">Whether or not the object is being disposed of.</param>
        protected virtual void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.Disposed)
            {
                // If disposing equals true, dispose all managed
                // and unmanaged resources.
                if (disposing)
                {
                    // Dispose managed resources.
                    if (this.DataReader != null)
                        this.DataReader.Close();

                    if (this.DbConnection != null)
                        this.DbConnection.Close();

                    this.DataReader = null;
                    this.DbConnection = null;
                }

                // Call the appropriate methods to clean up
                // unmanaged resources here.
                // If disposing is false,
                // only the following code is executed.

                // Note disposing has been done.
                Disposed = true;
            }
        }

        #endregion
    }
}