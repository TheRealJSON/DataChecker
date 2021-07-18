using DataCheckerProj.Importers;
using System;
using System.Collections.Generic;

namespace DataCheckerProj.Mapping
{
    public class TableMapping
    {
        #region properties

        public string SourceDatabaseName { get; set; }
        public string SourceSchemaName { get; set; }
        public string SourceTableName { get; set; }
        public string DestinationDatabaseName { get; set; }
        public string DestinationSchemaName { get; set; }
        public string DestinationTableName { get; set; }
        public List<ColumnMapping> MappedColumns;

        #endregion

        #region constructors
        public TableMapping()
        {
            this.MappedColumns = new List<ColumnMapping>();
        }

        #endregion 

        #region methods
        public void LoadMappingFromSqlServerDatabase(SqlServerImporter mappingImporter, Dictionary<string, string> propertyToSourceAttributeMap)
        {
            /*
             * Verify Params are not null 
             */
            if (mappingImporter == null)
            {
                throw new ArgumentException("Parameter mappingImporter cannot be null");
            }
            else if (propertyToSourceAttributeMap == null)
            {
                throw new ArgumentException("Parameter propertyToSourceAttributeMap cannot be null");
            }

            /* 
             * Verify propertyToSourceAttributeMap contains required Property name mappings 
             */


            /* 
             * Verify propertyToSourceAttributeMap contains source attribute names that match those returned by query used for provided SqlServerImporter
             */

            /*
            * Load mappings from source 
            */
        }

        public bool Validate()
        {
            return false;
        }

        #endregion
    }
}
