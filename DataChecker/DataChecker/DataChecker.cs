﻿using DataCheckerProj.ErrorHandling;
using DataCheckerProj.Helpers;
using DataCheckerProj.Mapping;
using DataCheckerProj.Sampling;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using static DataCheckerProj.Helpers.SqlQueryBuilder;

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
        /// The path of the windows folder that contains, or will contain, log files and output
        /// </summary>
        private string LogFileFolderPath;

        #endregion

        #region constructors

        public DataChecker()
        {
            this.MappingToBeChecked = null;
        }

        public DataChecker(TableMapping tableToBeChecked, string dataSourceConnectionString, string logFileFolderPath)
        {
            this.MappingToBeChecked = tableToBeChecked;
            this.DataSourceConnectionString = dataSourceConnectionString;
            this.LogFileFolderPath = logFileFolderPath;
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

            TableReference sourceSqlTableReference;
            TableReference destinationSqlTableReference;

            DataTable sourceSample;
            DataTable destinationSample;

            Dictionary<string, dynamic> firstRecordIdentityInSample; // <Identity_Column_Name, Identity_Column_Value>
            Dictionary<string, dynamic> lastRecordIdentityInSample; // <Identity_Column_Name, Identity_Column_Value>

            /* Prepare to sample old schema */
            sourceSqlTableReference = new TableReference(MappingToBeChecked.SourceDatabaseName, MappingToBeChecked.SourceSchemaName, MappingToBeChecked.SourceTableName);
            List<Condition> recordClassesToIgnore = GetConditionListOfClassesToIgnore();
            IDataSamplingStrategy sourceDataSampler = new SequentialDataSampler(new OdbcConnection(this.DataSourceConnectionString), sourceSqlTableReference, recordClassesToIgnore, srcIdentityColNames);

            while (sourceDataSampler.SampleDataSource().Rows.Count > 0) // keep sampling old schema
            {
                sourceSample = sourceDataSampler.LastSampleTaken; // execution of loop condition will have initiated a sample

                /* Find out bounds of sample for efficient search in new/dest schema when record ordering is the same */
                firstRecordIdentityInSample = new Dictionary<string, dynamic>();
                lastRecordIdentityInSample = new Dictionary<string, dynamic>();

                foreach (string srcIdentityColumnName in srcIdentityColNames) // for every identity column (source names instead of dest names because searching source sample)
                {
                    // add next column_name-to-column_value mapping for FIRST record. Use destination column name in mapping because record will be used to search destination.
                    firstRecordIdentityInSample.Add(srcToDestIdentityColNameMap[srcIdentityColumnName], sourceSample.Rows[0][srcIdentityColumnName]);
                    // add next column_name-to-column_value mapping for LAST record. Use destination column name in mapping because record will be used to search destination. 
                    lastRecordIdentityInSample.Add(srcToDestIdentityColNameMap[srcIdentityColumnName], sourceSample.Rows[sourceSample.Rows.Count - 1][srcIdentityColumnName]);
                }

                /* Search for sample in new schema */
                destinationSqlTableReference = new TableReference(MappingToBeChecked.DestinationDatabaseName, MappingToBeChecked.DestinationSchemaName, MappingToBeChecked.DestinationTableName);
                IDataSamplingStrategy destinationDataSampler = SequentialDataSampler.GetBoundedSequentialDataSampler(new OdbcConnection(this.DataSourceConnectionString), destinationSqlTableReference, firstRecordIdentityInSample, lastRecordIdentityInSample, srcToDestIdentityColNameMap.Values.ToList()); // sampler should be instantiated with same ordering as sourceSampler

                destinationSample = destinationDataSampler.SampleDataSource(); // sampler should query records in same order as sourceDataSampler

                // Now we should have a sample from old/src schema and new/dest schema
                // Both samples should share the same boundary records and therefore, due to common ordering, should share records
                // So now lets verify no records are missing from new/dest schema
                DataTable missingDestinationRows = DataTableHelper.GetDataTablesLeftDisjoint(sourceSample, destinationSample, new CustomDataRowComparer(srcToDestIdentityColNameMap)); //get records in sourceSample but not destinationSample

                /* If there's an issue report it */
                if (missingDestinationRows.Rows.Count > 0)
                {
                    problemsFound = true;
                    ReportMissingRecords(missingDestinationRows, srcIdentityColNames);
                }
            }

            return problemsFound;
        }

        /// <summary>
        /// Get a list of <c>Conditions</c> that define classes of record to ignore based on information taken from the <c>MappingToBeChecked</c> property.
        /// </summary>
        /// <returns>A list of <c>Conditions</c> that specify column values to ignore</returns>
        private List<Condition> GetConditionListOfClassesToIgnore()
        {
            List<Condition> conditionsForClassesToIgnore = new List<Condition>();
            foreach (ColumnMapping colMap in MappingToBeChecked.MappedColumns)
            {
                foreach (string decomissionedClass in colMap.DecommissionedClasses) // in Home Office context, only string/varchar fields contain classes (event_type)
                {
                    conditionsForClassesToIgnore.Add(new Condition(colMap.SourceColumnName, "<>", decomissionedClass));
                }
            }

            return conditionsForClassesToIgnore;
        }

        /// <summary>
        /// Logs the identities of records that have been found to be missing from the destination table defined in the  <c>MappingToBeChecked</c> property.
        /// </summary>
        /// <param name="missingDestinationRecords">Records to report as missing</param>
        /// <param name="sourceIdentityColNames">The names of the identity columns in the context of the source table</param>
        private void ReportMissingRecords(DataTable missingDestinationRecords, List<string> sourceIdentityColNames)
        {
            string sourceTableBeingVerified = MappingToBeChecked.GetSourceTableReferenceForFilePath();
            string mappingInfo = sourceTableBeingVerified.Replace("-", ".") + " to " + MappingToBeChecked.DestinationSchemaName + "." + MappingToBeChecked.DestinationTableName;

            LogWriter.LogError("DataChecker.ReportMissingRecords()", sourceTableBeingVerified, "Some records were found to be MISSING from the destination schema!", mappingInfo, LogFileFolderPath);

            // not implemented yet
            foreach (DataRow row in missingDestinationRecords.Rows)
            {
                string recordIdentity = "";
                foreach (string srcIdentityColumn in sourceIdentityColNames)
                {
                    recordIdentity += srcIdentityColumn + ":= " + row[srcIdentityColumn].ToString() + " ; ";
                }

                LogWriter.LogError("DataChecker.ReportMissingRecords()", sourceTableBeingVerified, recordIdentity, mappingInfo, LogFileFolderPath);
            }
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