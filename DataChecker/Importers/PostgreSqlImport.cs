
using System;
using System.Data;

using System.Data.Odbc;
using DataCheckerProj.ErrorHandling;
using DataCheckerProj.Mapping;

namespace DataCheckerProj.Importers
{
    class PostgreSqlImport : DataImporter
    {
        private OdbcConnection DbConnection; // Maintain this connection
        private OdbcDataReader DataReader; // Used to import data via the DbConnection

        public string TableName;
        public string TableSchema;

        #region Constructors

        public PostgreSqlImport(OdbcConnection importConnection, TableMapping attributeMap, DateTimeOffset? timestampToLoadFrom, int? rowLimit, string logFilePath)
        {
            this.ConnectionString = importConnection.ConnectionString;
            this.LogFilePath = logFilePath;
            this.DbConnection = importConnection;
            this.TableSchema = attributeMap.SourceSchemaName;
            this.TableName = attributeMap.SourceTableName;

            InitialiseDataReader(timestampToLoadFrom, rowLimit);
            InitialiseDataSegment(attributeMap);
        }

        /// <summary>
        /// Creates an instance of the ADO.Net OdbcDataReader to be used 
        /// by the class during its lifespan for importing/reading data.
        /// </summary>
        /// <param name="timestampToLoadFrom">timestamp that loaded records should have as a minimum</param>
        /// <param name="rowLimit">a row limit to apply to imported data</param>
        private void InitialiseDataReader(DateTimeOffset? timestampToLoadFrom, int? rowLimit)
        {
            string tableReference = this.TableSchema + "." + this.TableName;

            try
            {
                if (this.DbConnection.State != ConnectionState.Open)
                    this.DbConnection.Open();

                string importQuery = string.Format("", tableReference);

                if (timestampToLoadFrom != null)
                    importQuery += " WHERE timestamp >= '" + timestampToLoadFrom.Value.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'"; // >= because rows might be loaded after max_timestamp logged with same timestamp

                if (rowLimit != null)
                    importQuery += " LIMIT " + rowLimit;

                using (OdbcCommand command = new OdbcCommand(importQuery, this.DbConnection))
                {
                    command.CommandTimeout = 1200;
                    this.DataReader = command.ExecuteReader();
                }

                if (this.DataReader == null) // only reason this could happen is source doesn't exist?
                    throw new Exception("Source " + tableReference + " does not exist");

            }
            catch (Exception ex)
            {
                LogWriter.LogError("initialiseDataReader", this.TableName, ex.Message, timestampToLoadFrom.ToString(), this.LogFilePath);
                throw ex;
            }
        }

        /// <summary>
        /// Builds the column structure of the DataTable used
        /// for storing segments of data, by querying the source table.
        /// </summary>
        /// <param name="dataMapping">Table and column mappings</param> 
        private void InitialiseDataSegment(TableMapping dataMapping)
        {
            try
            {
                if (this.DbConnection.State != ConnectionState.Open)
                    this.DbConnection.Open();

                string colStructureQuery = string.Format("", this.TableSchema + "." + this.TableName, 1);

                // Get table definition dynamically
                using (OdbcDataAdapter tmpReader = new OdbcDataAdapter(colStructureQuery, this.DbConnection))
                {
                    tmpReader.Fill(this.DataSegment);   // Use fill just to get column definitions
                    this.DataSegment.Clear();           // Delete row left over from query used 
                }

                /* Fix for Timezone offsets being added to DateTime timestamps when inserted to Sql Server */
                /* Fix for datetimeoffset to nvarchar mappings not working */
                DataTable adjustedTable = new DataTable();
                for (int i = 0; i < DataSegment.Columns.Count; i++)
                {
                    Type colType;

                    if (DataSegment.Columns[i].DataType == System.Type.GetType("System.DateTime")) // Is this guaranteed to catch all Datetime with offset (PG) types?
                    {
                        colType = System.Type.GetType("System.DateTimeOffset");
                    }
                    else
                    {
                        colType = DataSegment.Columns[i].DataType;
                    }

                    adjustedTable.Columns.Add(DataSegment.Columns[i].ColumnName, colType);
                }

                this.DataSegment = adjustedTable;
            }
            catch (Exception ex)
            {
                LogWriter.LogError("initialiseDataSegment", this.TableName, ex.Message, "no data available", this.LogFilePath);
                throw ex;
            }
        }

        #endregion

        #region Methods


        /// <summary>
        /// Retrieves a sequential subset of rows from the target
        /// source table.
        /// </summary>
        /// <param name="rowLimit">The row limit for the data segment being retrieved.</param> 
        /// <returns>
        /// A DataTable containing rows from the target source table which 
        /// occur after the last set of rows processed.
        /// </returns>
        public override DataTable NextDataSegment(int rowLimit)
        {
            try
            {
                if (this.DbConnection.State != ConnectionState.Open)
                    this.DbConnection.Open();

                DataTable nextDataSegment = this.DataSegment.Clone(); // Clone to get column defintions
                System.Type importedDateTimeType = System.Type.GetType("System.DateTime");
                object[] colArray = new object[this.DataReader.FieldCount];
                int rowCount = 0;
                while (rowCount < rowLimit && this.DataReader.Read())
                {
                    try // TRY to import/load next row
                    {
                        for (int i = 0; i < this.DataReader.FieldCount; i++) // build a new data row
                        {
                            if (this.DataReader[i].GetType() == importedDateTimeType) // if current col is a DateTime column
                            {
                                colArray[i] = new DateTimeOffset((System.DateTime)this.DataReader[i], new TimeSpan(0, 0, 0)); //Enforce +00:00 UTC offset
                            }
                            else
                            {
                                colArray[i] = this.DataReader[i];
                            }
                        }

                        nextDataSegment.LoadDataRow(colArray, true); // add new data row to data segment
                        rowCount++;
                    }
                    catch (Exception e) // Unable to import/load row
                    {
                        LogWriter.LogError("NextDataSegment", this.TableName, e.Message, "rowCount: " + rowCount, this.LogFilePath); //report and continue
                        throw e;
                    }
                }

                this.DataSegment = nextDataSegment; // transaction complete, this segment is now the current segment
            }
            catch (Exception ex)
            {
                LogWriter.LogError("NextDataSegment", this.TableName, ex.Message, "", this.LogFilePath);
                throw ex;
            }

            return this.DataSegment;
        }

        public override void Dispose()
        {
            if (this.DataReader != null)
                this.DataReader.Close();

            this.DbConnection.Close();
        }

        #endregion
    }
}

