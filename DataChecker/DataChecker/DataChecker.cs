using DataCheckerProj.Helpers;
using DataCheckerProj.Mapping;
using DataCheckerProj.Sampling;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Linq;

namespace DataCheckerProj
{
    /// <summary>
    /// Class used to perform automated reconciliation testing activites against a source and destination schema defined by the <c>MappingToBeChecked</c> field.
    /// </summary>
    public class DataChecker
    {
        #region properties 

        /// <summary>
        /// Database connection string used for the data source connection
        /// </summary>
        private string DataSourceConnectionString;

        /// <summary>
        /// The TableMapping that links the source table to the destination table to reconcile superficial differences like column name changes
        /// </summary>
        private TableMapping MappingToBeChecked;

        /// <summary>
        /// The table for inserting log information into
        /// </summary>
        private SqlQueryBuilder.TableReference LogTableReference;

        /// <summary>
        /// The connection for the database that contains the log table
        /// </summary>
        private SqlConnection LogTableConnection;

        #endregion

        #region constructors

        public DataChecker()
        {
            this.MappingToBeChecked = null;
        }

        public DataChecker(TableMapping tableToBeChecked, string dataSourceConnectionString, SqlQueryBuilder.TableReference logTable, SqlConnection sqlConn)
        {
            this.MappingToBeChecked = tableToBeChecked;
            this.DataSourceConnectionString = dataSourceConnectionString;
            this.LogTableReference = logTable;
            this.LogTableConnection = sqlConn;
        }

        #endregion

        #region methods

        /// <summary>
        /// Verifies that records from the source table can still be found in the destination table and reports/logs any records that are unaccounted for. 
        /// Source and destination tables are linked and defined by the <c>MappingToBeChecked</c> field. 
        /// </summary>
        /// <returns>Whether or not problems (missing records) have been found (and reported)</returns>
        public bool VerifyDataMatchesBetweenSourceAndDestination()
        {
            bool problemsFound = false;

            Dictionary<string, string> srcToDestIdentityColNameMap = MappingToBeChecked.MappedColumns.FindAll(c => c.IsIdentityColumn) // get all identity columns
                                                                                                     .ToDictionary(c => c.SourceColumnName, c => c.DestinationColumnName); // map Source_Col_Name to Destination_Col_Name
            List<string> srcIdentityColNames = srcToDestIdentityColNameMap.Keys.ToList();
            ColumnMapping identityColumnToOrderBy = MappingToBeChecked.MappedColumns.Find(c => c.IsOrderByColumn);

            SqlQueryBuilder.TableReference sourceSqlTableReference;
            SqlQueryBuilder.TableReference destinationSqlTableReference;

            DataTable sourceSample;

            Dictionary<string, dynamic> firstRecordIdentityInSample; // <Identity_Column_Name, Identity_Column_Value>
            Dictionary<string, dynamic> lastRecordIdentityInSample; // <Identity_Column_Name, Identity_Column_Value>

            /* Prepare to sample old schema */
            sourceSqlTableReference = new SqlQueryBuilder.TableReference(MappingToBeChecked.SourceDatabaseName, MappingToBeChecked.SourceSchemaName, MappingToBeChecked.SourceTableName);
            List<SqlQueryBuilder.Condition> recordClassesToIgnore = GetConditionListOfClassesToIgnore();
            IDataSamplingStrategy sourceDataSampler = new SequentialDataSampler(new OdbcConnection(this.DataSourceConnectionString), sourceSqlTableReference, recordClassesToIgnore, new List<string>() { identityColumnToOrderBy.SourceColumnName });

            while (sourceDataSampler.SampleDataSource().Rows.Count > 0) // keep sampling old schema
            {
                sourceSample = sourceDataSampler.LastSampleTaken; // execution of loop condition will have initiated a sample

                /* Find out bounds of sample for efficient search in new/dest schema when record ordering is the same */
                firstRecordIdentityInSample = new Dictionary<string, dynamic>();
                lastRecordIdentityInSample = new Dictionary<string, dynamic>();

                // add next column_name-to-column_value mapping for FIRST record. Use destination column name in mapping because record will be used to search destination.
                firstRecordIdentityInSample.Add(identityColumnToOrderBy.DestinationColumnName, sourceSample.Rows[0][identityColumnToOrderBy.SourceColumnName]);
                // add next column_name-to-column_value mapping for LAST record. Use destination column name in mapping because record will be used to search destination. 
                lastRecordIdentityInSample.Add(identityColumnToOrderBy.DestinationColumnName, sourceSample.Rows[sourceSample.Rows.Count - 1][identityColumnToOrderBy.SourceColumnName]);

                /* Search for sample in new schema */
                destinationSqlTableReference = new SqlQueryBuilder.TableReference(MappingToBeChecked.DestinationDatabaseName, MappingToBeChecked.DestinationSchemaName, MappingToBeChecked.DestinationTableName);
                IDataSamplingStrategy destinationDataSampler = SequentialDataSampler.GetBoundedSequentialDataSampler(new OdbcConnection(this.DataSourceConnectionString),
                                                                                                                        destinationSqlTableReference,
                                                                                                                        firstRecordIdentityInSample,
                                                                                                                        lastRecordIdentityInSample,
                                                                                                                        new List<string>() { identityColumnToOrderBy.DestinationColumnName }); // sampler should be instantiated with same ordering as sourceSampler

                //destinationSample = destinationDataSampler.SampleDataSource(); // sampler should query records in same order as sourceDataSampler
                while (destinationDataSampler.SampleDataSource().Rows.Count > 0)
                {
                    // Now we should have a sample from old/src schema and new/dest schema
                    // Both samples should share the same boundary records and therefore, due to common ordering, should share records
                    // So now lets verify no records are missing from new/dest schema
                    DataTable missingDestinationRows = DataTableHelper.GetDataTablesLeftDisjoint(sourceSample, destinationDataSampler.LastSampleTaken, new CustomDataRowComparer(srcToDestIdentityColNameMap)); //get records in sourceSample but not destinationSample

                    /* If there's an issue report it */
                    if (missingDestinationRows.Rows.Count > 0)
                    {
                        problemsFound = true;
                        ReportMissingRecords(missingDestinationRows, srcIdentityColNames);
                    }
                }
            }

            return problemsFound;
        }

        /// <summary>
        /// Get a list of <c>Conditions</c> that define classes of record to ignore based on information taken from the <c>MappingToBeChecked</c> property.
        /// </summary>
        /// <returns>A list of <c>Conditions</c> that specify column values to ignore</returns>
        private List<SqlQueryBuilder.Condition> GetConditionListOfClassesToIgnore()
        {
            List<SqlQueryBuilder.Condition> sqlConditionsForClassesToIgnore = new List<SqlQueryBuilder.Condition>();
            foreach (ColumnMapping colMap in MappingToBeChecked.MappedColumns) // for each column on the table this class is "checking"
            {
                // in Home Office context, only string/varchar fields contain classes (event_type)
                foreach (string decomissionedClass in colMap.DecommissionedClasses) // for each value that should indicate a decommissioned record
                {                                                                   // info about decommissioned classes of record should already be populated
                    // transform current decommissioned class, as described by the TableMapping/ColumnMapping, into a Query (WHERE) Condition
                    SqlQueryBuilder.Condition whereRecordClassIsNotDecommissioned = new SqlQueryBuilder.Condition(colMap.SourceColumnName, "<>", decomissionedClass);

                    // add additional condition to return records when the column containing the decommissioned class IS NULL (fixes a problem with Postrgresql)
                    if (colMap.SourceColumnType.Equals("nvarchar")) //TODO: remove hardcoded type
                    {
                        whereRecordClassIsNotDecommissioned.replaceNulls = true;
                    }

                    sqlConditionsForClassesToIgnore.Add(whereRecordClassIsNotDecommissioned);
                }
            }

            return sqlConditionsForClassesToIgnore;
        }

        /// <summary>
        /// Logs the identities of records that have been found to be missing from the destination table defined in the  <c>MappingToBeChecked</c> property.
        /// </summary>
        /// <param name="missingDestinationRecords">Records to report as missing</param>
        /// <param name="sourceIdentityColNames">The names of the identity columns in the context of the source table</param>
        private void ReportMissingRecords(DataTable missingDestinationRecords, List<string> sourceIdentityColNames)
        {
            if (this.LogTableConnection.State != ConnectionState.Open)
                this.LogTableConnection.Open();

            string mappingBeingVerified = MappingToBeChecked.ToString();

            /* Construct columns used by log table */
            string uniquePrefix = "DataChecker";
            string srcSchemaColumnName = uniquePrefix + "_src_schema"; // prefix all cols with DataChecker to prevent overlap with existing columns
            string srcTableColumnName = uniquePrefix + "_src_table";
            string dstSchemaColumnName = uniquePrefix + "_dst_schema";
            string dstTableColumnName = uniquePrefix + "_dst_table";
            string messageTypeColumnName = uniquePrefix + "_message_type";
            string messageColumnName = uniquePrefix + "_Record_Identity";

            missingDestinationRecords.Columns.Add(srcSchemaColumnName, typeof(string));
            missingDestinationRecords.Columns.Add(srcTableColumnName, typeof(string));
            missingDestinationRecords.Columns.Add(dstSchemaColumnName, typeof(string));
            missingDestinationRecords.Columns.Add(dstTableColumnName, typeof(string));
            missingDestinationRecords.Columns.Add(messageTypeColumnName, typeof(string));
            missingDestinationRecords.Columns.Add(messageColumnName, typeof(string));

            for (int i = 0; i < missingDestinationRecords.Rows.Count; i++) // loop over rows and set the columns used by log table
            {
                string recordIdentity = "";

                // convert identity of current record into text message
                foreach (string srcIdentityColumn in sourceIdentityColNames)
                {
                    recordIdentity += srcIdentityColumn + ":= " + missingDestinationRecords.Rows[i][srcIdentityColumn].ToString() + " ; ";
                }

                missingDestinationRecords.Rows[i][srcSchemaColumnName] = this.MappingToBeChecked.SourceSchemaName;
                missingDestinationRecords.Rows[i][srcTableColumnName] = this.MappingToBeChecked.SourceTableName;
                missingDestinationRecords.Rows[i][dstSchemaColumnName] = this.MappingToBeChecked.DestinationSchemaName;
                missingDestinationRecords.Rows[i][dstTableColumnName] = this.MappingToBeChecked.DestinationTableName;
                missingDestinationRecords.Rows[i][messageTypeColumnName] = "missing_record";
                missingDestinationRecords.Rows[i][messageColumnName] = recordIdentity;
            }

            /* Remove all columns except columns used by log table */
            for (int i = 0; i < missingDestinationRecords.Columns.Count; i++)
            {
                if (missingDestinationRecords.Columns[i].ColumnName.Contains(uniquePrefix) == false) // if column is not one added by this method
                {
                    missingDestinationRecords.Columns.RemoveAt(i);
                }
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(this.LogTableConnection))
            {
                bulkCopy.ColumnMappings.Add(srcSchemaColumnName, "src_schema");
                bulkCopy.ColumnMappings.Add(srcTableColumnName, "src_table");
                bulkCopy.ColumnMappings.Add(dstSchemaColumnName, "dst_schema");
                bulkCopy.ColumnMappings.Add(dstTableColumnName, "dst_table");
                bulkCopy.ColumnMappings.Add(messageTypeColumnName, "message_type");
                bulkCopy.ColumnMappings.Add(messageColumnName, "message");

                bulkCopy.BulkCopyTimeout = 600;
                bulkCopy.DestinationTableName = string.Format("[{0}].[{1}].[{2}]", LogTableReference.DatabaseName,
                                                                                    LogTableReference.SchemaName,
                                                                                    LogTableReference.TableName);
                bulkCopy.WriteToServer(missingDestinationRecords);
            }
            /* Write missing records to log table */
        }

        #endregion

        public class CustomDataRowComparer : IEqualityComparer<DataRow>
        {
            Dictionary<string, string> srcToDestIdentityColNameMap;

            public CustomDataRowComparer(Dictionary<string, string> identityColMap)
            {
                this.srcToDestIdentityColNameMap = identityColMap;
            }

            public bool Equals(DataRow dst, DataRow src)
            {
                bool match = true;

                for (int i = 0; i < srcToDestIdentityColNameMap.Count; i++) // foreach identity column
                {
                    if (src[srcToDestIdentityColNameMap.ElementAt(i).Key].Equals(dst[srcToDestIdentityColNameMap.ElementAt(i).Value]) == false)
                    {
                        match = false; // different because different identity column values
                    }
                }

                return match;
            }

            public int GetHashCode(DataRow row)
            {
                //Check whether the object is null
                if (Object.ReferenceEquals(row, null)) return 0;

                //Get every identity column's hash code
                int rowHashCode = 0;
                for (int i = 0; i < srcToDestIdentityColNameMap.Count; i++) // foreach identity column
                {
                    // we don't know if its src or dst row, therefore don't know which Names are used for identity columns
                    if (row.Table.Columns.Contains(srcToDestIdentityColNameMap.ElementAt(i).Key)) // IF source row (contains src identity column name)
                    {
                        rowHashCode = rowHashCode ^ row[srcToDestIdentityColNameMap.ElementAt(i).Key].GetHashCode();
                    }
                    else // else must be destination row (doesn't contain the src column name)
                    {
                        rowHashCode = rowHashCode ^ row[srcToDestIdentityColNameMap.ElementAt(i).Value].GetHashCode();
                    }
                }

                return rowHashCode;
            }
        }
    }
}