using DataCheckerProj.ErrorHandling;
using DataCheckerProj.Helpers;
using DataCheckerProj.Importers;
using DataCheckerProj.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using static DataCheckerProj.Helpers.SqlQueryBuilder;

namespace DataCheckerProj
{
    class Program
    {
        private static string PostgresqlConnectionString;               // used for reading data to verify
        private static string SqlServerConnectionString;                // used to read configuration data
        private static string LogFileFolderPath;                        // used for reporting
        private static TableReference MappingDataSource;                // the SQL table containing schema mapping information
        private static TableReference DecommissionedClassesDataSource;  // the SQL table containing information about record classes that have been removed from future datasets
        private static bool ParametersLoaded = false;                   // whether or not all parameters have been loaded without error

        private static List<TableMapping> TableMappingList = new List<TableMapping>(); // list of schema mapping information used for processing

        static void Main(string[] args)
        {
            bool initialisationSuccess = Initialise(); // initialise() should report invalid params

            if (initialisationSuccess)
            {
                foreach (TableMapping tableMappingToVerify in TableMappingList)
                {
                    string sourceTableBeingVerified = tableMappingToVerify.GetSourceTableReferenceForFilePath(); // used for reporting/log writing

                    try
                    {
                        DataChecker checker = new DataChecker(tableMappingToVerify, PostgresqlConnectionString, LogFileFolderPath); // this class uses passed mapping info to cross-verify data between schemas

                        bool problemsFound = checker.VerifyDataMatchesBetweenSourceAndDestination(); // verifies no data elements are missing between source and destination schema

                        if (problemsFound) // specifics of problems should be reported by checker class
                            Dts.Events.FireError(0, "Main(string[] args)", "Process found discrepencies between source and destination data for source table " + sourceTableBeingVerified, String.Empty, 0);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.LogError("Main(string[] args)", sourceTableBeingVerified, ex.ToString(), "no data description available", LogFileFolderPath); // can use LogWriter because initialisationSuccess=true :D
                    }
                }
            }
            else
            {
                Dts.Events.FireError(0, "Main(string[] args)", "Failed to initialise. Terminating.", String.Empty, 0);
            }
        }

        /// <summary>
        /// Loads user parameters, loads table and attribute mappings, 
        /// loads record classes to exclude from processing (Decommissioned).
        /// Bootstraps initial state needed before processing begins. 
        /// </summary>
        /// <returns>success true/false</returns>
        private static bool Initialise()
        {
            bool initialiseSuccess = false;

            /* v REMOVE WHEN DEPLOYED v */
            Dts.BootstrapFakeDtsClass();  // Called to mock Dts object when not deployed to SSIS package
            /* ^ REMOVE WHEN DEPLOYED ^ */

            ParametersLoaded = LoadParameters();

            if (ParametersLoaded)
            {
                try // try to load the schema mappings
                {
                    bool loadMappingsSuccess = LoadAllMappings();

                    if (loadMappingsSuccess)
                    {
                        bool loadDecommissionedRecordClassesSuccess = LoadDecommissionedRecordClasses();

                        if (loadDecommissionedRecordClassesSuccess)
                        {
                            initialiseSuccess = true; // we have now successfully loaded params, mappings, and record classes to ignroe
                        }
                        else
                        {
                            initialiseSuccess = false;
                            Dts.Events.FireError(0, "Initialise()", "Failed to load decommissioned record classes.", String.Empty, 0);
                        }
                    }
                    else
                    {
                        initialiseSuccess = false;
                        Dts.Events.FireError(0, "Initialise()", "Failed to load list of table mappings to verify.", String.Empty, 0);
                    }
                }
                catch (Exception ex)
                {
                    Dts.Events.FireError(0, "Initialise()", "Failed to load list of table mappings to verify: " + ex.ToString(), String.Empty, 0);
                }
            }
            else
            {
                initialiseSuccess = false;
            }

            return initialiseSuccess;
        }

        /// <summary>
        /// Reads user-input parameters and saves their values to fields.
        /// </summary>
        /// <returns>success true/false</returns>
        private static bool LoadParameters()
        {
            bool success;

            try
            {
                List<string> invalidParameters = new List<string>();

                LogFileFolderPath = GetAndValidateStringParameter("Log_File_Folder_Path", ref invalidParameters);

                string dataSourceDatabase = GetAndValidateStringParameter("Decommissioned_Classes_Source__Database", ref invalidParameters);
                string dataSourceSchema = GetAndValidateStringParameter("Decommissioned_Classes_Source__Schema", ref invalidParameters);
                string dataSourceTable = GetAndValidateStringParameter("Decommissioned_Classes_Source__Table", ref invalidParameters);
                DecommissionedClassesDataSource = new TableReference(dataSourceDatabase, dataSourceSchema, dataSourceTable);

                dataSourceDatabase = GetAndValidateStringParameter("Mapping_Data_Source__Database", ref invalidParameters);
                dataSourceSchema = GetAndValidateStringParameter("Mapping_Data_Source__Schema", ref invalidParameters);
                dataSourceTable = GetAndValidateStringParameter("Mapping_Data_Source__Table", ref invalidParameters);
                MappingDataSource = new TableReference(dataSourceDatabase, dataSourceSchema, dataSourceTable);

                PostgresqlConnectionString = GetAndValidateStringParameter("Postgresql_Connection_String", ref invalidParameters);
                SqlServerConnectionString = GetAndValidateStringParameter("SqlServer_Connection_String", ref invalidParameters);

                if (invalidParameters.Count > 0)
                {
                    Dts.Events.FireError(0, "LoadParameters()", "The following parameters are invalid: " + invalidParameters.ToString(), String.Empty, 0);
                    success = false;
                }

                success = true; // no problems
            }
            catch (Exception ex)
            {
                Dts.Events.FireError(0, "LoadParameters()", "There was a problem loading the parameters: " + ex.ToString(), String.Empty, 0);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Reads the table attribute mappings essential for processing.
        /// Uses a data source that should be provided by user via parameters. 
        /// Uses a variety of configurations. 
        /// </summary>
        /// <returns>success true/false</returns>
        public static bool LoadAllMappings()
        {
            bool loadMappingsSuccess = false;

            if (ParametersLoaded)
            {
                TableMappingList.Clear(); // remove any current mappings in preparation for loading them from scratch

                Dictionary<string, string> mappingPropertyToSourceAttributeMap = LoadMappingPropertyToSourceAttributeMapFromConfig(); // mapping class properties to Sql Table attributes
                List<Dictionary<string, string>> mappingsToLoadList = LoadMappingsToVerifyFromConfig(MappingDataSource, mappingPropertyToSourceAttributeMap); // get list of mappings that should be loaded for processing

                foreach (Dictionary<string, string> mappingToLoad in mappingsToLoadList)
                {
                    bool loadSingleMappingSucess = LoadTableMapping(MappingDataSource, mappingPropertyToSourceAttributeMap, mappingToLoad["SourceSchemaName"],
                                                                                                                            mappingToLoad["SourceTableName"],
                                                                                                                            mappingToLoad["DestinationSchemaName"],
                                                                                                                            mappingToLoad["DestinationTableName"]);
                    if (loadSingleMappingSucess)
                    {
                        loadMappingsSuccess = true;
                    }
                    else
                    {
                        loadMappingsSuccess = false; // there was a problem loading 1 of the mappings so overall process failure
                        TableMappingList.Clear(); // clear mappings that were loaded before failure to prevent half-complete state
                        break; // stop trying to load further mappings because we have failed at some point
                    }
                }
            }
            else
            {
                throw new ArgumentException("Parameters must be successfully loaded before invoking LoadAllMappings()");
            }

            return loadMappingsSuccess;
        }

        /// <summary>
        /// Reads mapping data for a specified table from a specified SQL table source
        /// and saves the results to a class property for later processing. 
        /// Only a single table mapping (TableMapping.cs) is loaded, therefore, the details 
        /// needed to uniquely identify the table mapping must be specified via the parameters. 
        /// </summary>
        /// <param name="tableMappingSource">SQL table data source</param>
        /// <param name="mappingPropertyToSourceAttributeMap">Map of the Class property names to their corresponding source attribute names</param>
        /// <param name="mappingToLoadSourceSchema">The source schema specified by the mapping records to read</param>
        /// <param name="mappingToLoadSourceTable">The source table specified by the mapping records to read</param>
        /// <param name="mappingToLoadDestSchema">The destination schema specified by the mapping records to read</param>
        /// <param name="mappingToLoadDestTable">The destination table specified by the mapping records to read</param>
        /// <returns>success true/false</returns>
        private static bool LoadTableMapping(TableReference tableMappingSource, Dictionary<string, string> mappingPropertyToSourceAttributeMap, string mappingToLoadSourceSchema, string mappingToLoadSourceTable, string mappingToLoadDestSchema, string mappingToLoadDestTable)
        {
            bool success = false;

            try
            {
                List<string> tableMappingSourceAttributes = new List<string>(mappingPropertyToSourceAttributeMap.Values);

                SqlServerQueryBuilder queryBuilder = new SqlServerQueryBuilder();
                queryBuilder.Select(tableMappingSourceAttributes)
                            .From(tableMappingSource)
                            .Where(new Condition(mappingPropertyToSourceAttributeMap["SourceSchemaName"], "=", mappingToLoadSourceSchema))
                            .Where(new Condition(mappingPropertyToSourceAttributeMap["SourceTableName"], "=", mappingToLoadSourceTable))
                            .Where(new Condition(mappingPropertyToSourceAttributeMap["DestinationSchemaName"], "=", mappingToLoadDestSchema))
                            .Where(new Condition(mappingPropertyToSourceAttributeMap["DestinationTableName"], "=", mappingToLoadDestTable));

                string importQuery = queryBuilder.GetSelectQuery();

                using (SqlConnection conn = new SqlConnection(SqlServerConnectionString))
                {
                    SqlServerImporter mappingImporter = new SqlServerImporter(conn, importQuery);
                    TableMapping loadedMapping = new TableMapping();

                    loadedMapping.LoadMappingFromSqlServerDatabase(mappingImporter, mappingPropertyToSourceAttributeMap); // Fill new mapping object with data read from DB

                    if (loadedMapping.Validate())
                    {
                        TableMappingList.Add(loadedMapping);
                        success = true;
                    }
                    else
                    {
                        throw new EvaluateException("Loaded mapping information is invalid: " + mappingToLoadSourceSchema + "." + mappingToLoadSourceTable + " to " + mappingToLoadDestSchema + "." + mappingToLoadDestTable);
                    }
                }
            }
            catch (Exception e) // catch exceptions here so can report on specific table mapping that causes exception
            {
                string dataDescription = "tableMappingSource:= " + tableMappingSource.ToString() + "; ";
                dataDescription += "mappingPropertyToSourceAttributeMap:= ";
                foreach (KeyValuePair<string, string> propertyToAttribute in mappingPropertyToSourceAttributeMap)  // relevant info if LoadMappingFromSqlServerDatabase() threw argument exceptions
                {
                    dataDescription += propertyToAttribute.Key + "=" + propertyToAttribute.Value + "; ";
                }
                dataDescription += "mappingToLoad:= " + mappingToLoadSourceSchema + "." + mappingToLoadSourceTable + " to " + mappingToLoadDestSchema + "." + mappingToLoadDestTable;

                Dts.Events.FireError(0, "LoadTableMapping()", "Failed to load mapping... " + dataDescription, String.Empty, 0);

                success = false;
            }

            return success;
        }

        /// <summary>
        /// Reads the configuration for record classes/types that should be omitted from processing/checks. 
        /// If a record has a specified value in a specified column, as defined by the configuration, then it will be ignored. 
        /// </summary>
        /// <returns>success true/false</returns>
        private static bool LoadDecommissionedRecordClasses()
        {
            bool success = false;

            try
            {
                int colMappingIDPlaceholder = 0;
                List<string> columnsToSelect = new List<string>();
                columnsToSelect.Add("decommissioned_value");

                SqlServerQueryBuilder queryBuilder = new SqlServerQueryBuilder();
                queryBuilder.Select(columnsToSelect)
                            .From(DecommissionedClassesDataSource)
                            .Where(new Condition("column_mapping_id", "=", colMappingIDPlaceholder));

                string genericDecommissionedClassesQuery = queryBuilder.GetSelectQuery();

                using (SqlConnection conn = new SqlConnection(SqlServerConnectionString))
                {
                    foreach (TableMapping tableMapping in TableMappingList)
                    {
                        foreach (ColumnMapping columnMapping in tableMapping.MappedColumns)
                        {
                            string columnSpecificDecommissionedClassesQuery = genericDecommissionedClassesQuery.Replace(colMappingIDPlaceholder.ToString(), columnMapping.MappingID.ToString());

                            SqlServerImporter dataImporter = new SqlServerImporter(conn, columnSpecificDecommissionedClassesQuery);

                            List<string> decommissionedClasses = new List<string>();
                            while (dataImporter.NextDataSegment(500).Rows.Count > 0)
                            {
                                foreach (DataRow row in dataImporter.GetDataSegment().Rows)
                                {
                                    decommissionedClasses.Add(row["decommissioned_value"].ToString());
                                }
                            }

                            dataImporter.Dispose();
                            columnMapping.DecommissionedClasses = decommissionedClasses;
                        }
                    }
                }

                success = true;
            }
            catch (Exception e)
            {
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Creates a Dictionary that maps "Mapping" class property names to their corresponding data source attribute names
        /// so that source attribute values can be mapped to the correct properties.
        /// 
        /// Currently redundant, all done in code, mostly a placeholder method. Configuration of this is not a current need.
        /// </summary>
        /// <returns>Dictionary mapping property names to data source attribute names</returns>
        private static Dictionary<string, string> LoadMappingPropertyToSourceAttributeMapFromConfig()
        {
            // atm hardcoded because ability to configure is unecessary
            Dictionary<string, string> propertyToSourceAttributeMap = new Dictionary<string, string>();
            propertyToSourceAttributeMap.Add("SourceDatabaseName", "SourceDatabaseName");
            propertyToSourceAttributeMap.Add("SourceSchemaName", "SourceSchemaName");
            propertyToSourceAttributeMap.Add("SourceTableName", "SourceTableName");
            propertyToSourceAttributeMap.Add("DestinationDatabaseName", "DestinationDatabaseName");
            propertyToSourceAttributeMap.Add("DestinationSchemaName", "DestinationSchemaName");
            propertyToSourceAttributeMap.Add("DestinationTableName", "DestinationTableName");

            propertyToSourceAttributeMap.Add("MappingID", "mapping_id");
            propertyToSourceAttributeMap.Add("SourceColumnName", "SourceColumnName");
            propertyToSourceAttributeMap.Add("SourceColumnType", "SourceColumnType");
            propertyToSourceAttributeMap.Add("DestinationColumnName", "DestinationColumnName");
            propertyToSourceAttributeMap.Add("DestinationColumnType", "DestinationColumnType");
            propertyToSourceAttributeMap.Add("IsIdentityColumn", "IsIdentityColumn");

            return propertyToSourceAttributeMap;
        }

        /// <summary>
        /// Returns a list of Dictionaries where each Dictionary has entries for attributes that uniquely identify a Table Mapping.
        /// Each unique TableMapping identity that is represented in the return object is for a mapping that should be processed.
        /// Largely redundant in its current incarnation, partially a placeholder.
        /// </summary>
        /// <param name="mappingDataSource"></param>
        /// <param name="mappingPropertyToSourceAttributeMap"></param>
        /// <returns>List of dictionaires that uniquely identify Table Mappings to process</returns>
        private static List<Dictionary<string, string>> LoadMappingsToVerifyFromConfig(TableReference mappingDataSource, Dictionary<string, string> mappingPropertyToSourceAttributeMap)
        {
            /* build query that gets a list of table mappings that need verifying */
            List<string> uniqueTbleMappingIdentityCols = new List<string>(); // cols that conceptually identify a mapping, not necessarily primary key cols
            uniqueTbleMappingIdentityCols.Add(mappingPropertyToSourceAttributeMap["SourceSchemaName"]);
            uniqueTbleMappingIdentityCols.Add(mappingPropertyToSourceAttributeMap["SourceTableName"]);
            uniqueTbleMappingIdentityCols.Add(mappingPropertyToSourceAttributeMap["DestinationSchemaName"]);
            uniqueTbleMappingIdentityCols.Add(mappingPropertyToSourceAttributeMap["DestinationTableName"]);

            SqlServerQueryBuilder mappingsToVerifyQueryBuilder = BuildUniqueTableMappingsQuery(uniqueTbleMappingIdentityCols, mappingDataSource);

            Condition mappingIsForIdentityAttribute = new Condition(mappingPropertyToSourceAttributeMap["IsIdentityColumn"], "=", 1); // query Table Mappings that have column mappings associated to them for identity columns
            mappingsToVerifyQueryBuilder.Where(mappingIsForIdentityAttribute); // because current requirement is verifying no missing data elements, therefore need identity cols and nothing else

            /*
             * Read query results and convert to List<Dictionary<string, string>>,
             * where each Dictionary has entries for the attributes that identify a unique mapping.
             */
            List<Dictionary<string, string>> mappingsBeingVerified = new List<Dictionary<string, string>>();
            using (SqlConnection conn = new SqlConnection(SqlServerConnectionString))
            {
                SqlServerImporter configImporter = new SqlServerImporter(conn, mappingsToVerifyQueryBuilder.GetSelectQuery());
                while (configImporter.NextDataSegment(500).Rows.Count > 0)
                {
                    foreach (DataRow tableMappingRecord in configImporter.GetDataSegment().Rows)
                    {
                        Dictionary<string, string> mappingToVerify = new Dictionary<string, string>();
                        mappingToVerify.Add("SourceSchemaName", tableMappingRecord[mappingPropertyToSourceAttributeMap["SourceSchemaName"]].ToString());
                        mappingToVerify.Add("SourceTableName", tableMappingRecord[mappingPropertyToSourceAttributeMap["SourceTableName"]].ToString());
                        mappingToVerify.Add("DestinationSchemaName", tableMappingRecord[mappingPropertyToSourceAttributeMap["DestinationSchemaName"]].ToString());
                        mappingToVerify.Add("DestinationTableName", tableMappingRecord[mappingPropertyToSourceAttributeMap["DestinationTableName"]].ToString());

                        mappingsBeingVerified.Add(mappingToVerify);
                    }
                }
            }

            return mappingsBeingVerified;
        }

        private static SqlServerQueryBuilder BuildUniqueTableMappingsQuery(List<string> uniqueTableMappingIdentityCols, TableReference mappingDataSource)
        {
            SqlServerQueryBuilder queryBuilder = new SqlServerQueryBuilder();

            queryBuilder.Select(uniqueTableMappingIdentityCols, true)
                        .From(mappingDataSource);

            return queryBuilder;
        }

        private static string GetAndValidateStringParameter(string paramReference, ref List<string> invalidParameters)
        {
            string parameter = Dts.Variables[paramReference].Value.ToString();

            if (String.IsNullOrEmpty(parameter) || String.IsNullOrWhiteSpace(parameter))
            {
                invalidParameters.Add(paramReference);
            }

            return parameter;
        }

        public class Dts // Only here for code outside of SSIS solution. Remove when deploying to package. Testing purposes.
        {
            public static Dictionary<string, ValueObject> Variables = new Dictionary<string, ValueObject>();

            public static void BootstrapFakeDtsClass()
            {
                Variables.Add("Decommissioned_Classes_Source__Database", new ValueObject("master"));
                Variables.Add("Decommissioned_Classes_Source__Schema", new ValueObject("DataChecker"));
                Variables.Add("Decommissioned_Classes_Source__Table", new ValueObject("Decomissioned_Record_Classes"));
                Variables.Add("Mapping_Data_Source__Database", new ValueObject("master"));
                Variables.Add("Mapping_Data_Source__Schema", new ValueObject("DataChecker"));
                Variables.Add("Mapping_Data_Source__Table", new ValueObject("Table_Mapping"));
                Variables.Add("Log_File_Folder_Path", new ValueObject(@"C:\Users\Jaso\source\DataChecker\DataChecker\bin\logs\"));
                Variables.Add("Postgresql_Connection_String", new ValueObject("Dsn=PostgreSQL35W;"));
                Variables.Add("SqlServer_Connection_String", new ValueObject("Data Source=DESKTOP-LMMBET3;Initial Catalog=master;Integrated Security=True;"));
            }

            public class ValueObject
            {
                public dynamic Value;

                public ValueObject(dynamic val)
                {
                    this.Value = val;
                }
            }

            public class Events
            {
                public static void FireError(int errorCode, string subComponent, string description, string helpFile, int helpContext)
                {
                    Console.WriteLine(description);
                }
            }
        }
    }
}