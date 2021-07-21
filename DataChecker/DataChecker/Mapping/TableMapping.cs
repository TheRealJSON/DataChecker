﻿using DataCheckerProj.Importers;
using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.Data;

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
            //TODO: maybe add some verification for structure of results returned by importer

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
            List<string> propertyNamesInsideMap = propertyToSourceAttributeMap.Keys.ToList();

            PropertyInfo[] tableMappingPropertyInfos;
            PropertyInfo[] columnMappingPropertyInfos;
            tableMappingPropertyInfos = typeof(TableMapping).GetProperties(); // retrieve names of TableMapping class properties
            columnMappingPropertyInfos = typeof(ColumnMapping).GetProperties().Where(property => property.PropertyType != typeof(List<string>)).ToArray(); // retrieve names of ColumnMapping class properties, except List properties

            PropertyInfo[] allPropertyInfos = new PropertyInfo[tableMappingPropertyInfos.Length + columnMappingPropertyInfos.Length]; // combine arrays storing propery names into 1
            tableMappingPropertyInfos.CopyTo(allPropertyInfos, 0);
            columnMappingPropertyInfos.CopyTo(allPropertyInfos, tableMappingPropertyInfos.Length);

            for (int i = 0; i < allPropertyInfos.Length; i++) // for each property name that is required to be inside the provided name mappings
            {
                if (propertyNamesInsideMap.Contains(allPropertyInfos[i].Name) == false)
                {
                    throw new ArgumentException("Parameter propertyToSourceAttributeMap does not contain mapping for the property " + allPropertyInfos[i].Name);
                }
            }

            /* 
             * Verify propertyToSourceAttributeMap contains source attribute names that match those returned by query used for provided SqlServerImporter
             */
            List<string> srcAttributeNamesInsideMap = propertyToSourceAttributeMap.Values.ToList();

            foreach (DataColumn sourceAttribute in mappingImporter.GetDataSegment().Columns) // foreach attribute returned by query provided for importing mapping data
            {
                if (srcAttributeNamesInsideMap.Contains(sourceAttribute.ColumnName) == false) // if the provided dictionary does not contain a mapping for that data source attribute
                {
                    throw new ArgumentException("Parameter propertyToSourceAttributeMap does not contain mapping for the source attribute " + sourceAttribute.ColumnName);
                }
            }

            /*
            * Load mappings from source 
            */
            while (mappingImporter.NextDataSegment(500).Rows.Count > 0)
            {
                foreach (DataRow row in mappingImporter.GetDataSegment().Rows)
                {
                    this.SourceDatabaseName = row[propertyToSourceAttributeMap["SourceDatabaseName"]].ToString(); // unecessary to do this every iteration but nevermind
                    this.SourceSchemaName = row[propertyToSourceAttributeMap["SourceSchemaName"]].ToString(); // unecessary to do this every iteration but nevermind
                    this.SourceTableName = row[propertyToSourceAttributeMap["SourceTableName"]].ToString(); // unecessary to do this every iteration but nevermind
                    this.DestinationDatabaseName = row[propertyToSourceAttributeMap["DestinationDatabaseName"]].ToString(); // unecessary to do this every iteration but nevermind
                    this.DestinationSchemaName = row[propertyToSourceAttributeMap["DestinationSchemaName"]].ToString(); // unecessary to do this every iteration but nevermind
                    this.DestinationTableName = row[propertyToSourceAttributeMap["DestinationTableName"]].ToString(); // unecessary to do this every iteration but nevermind

                    MappedColumns.Add(new ColumnMapping(Convert.ToInt32(row[propertyToSourceAttributeMap["MappingID"]]),
                                                        row[propertyToSourceAttributeMap["SourceColumnName"]].ToString(),
                                                        row[propertyToSourceAttributeMap["SourceColumnType"]].ToString(),
                                                        row[propertyToSourceAttributeMap["DestinationColumnName"]].ToString(),
                                                        row[propertyToSourceAttributeMap["DestinationColumnType"]].ToString(),
                                                        Convert.ToBoolean(row[propertyToSourceAttributeMap["IsIdentityColumn"]])));
                }
            }
        }

        public bool Validate()
        {
            bool isValid = true;

            List<string> usedDestCols = new List<string>();
            List<string> usedSrcCols = new List<string>();
            foreach (ColumnMapping colMap in this.MappedColumns)
            {
                /* Check for duplicate columns */
                if (usedDestCols.Contains(colMap.DestinationColumnName))
                    isValid = false;
                usedDestCols.Add(colMap.DestinationColumnName);

                if (usedSrcCols.Contains(colMap.SourceColumnName))
                    isValid = false;
                usedSrcCols.Add(colMap.SourceColumnName);
            }

            if (this.MappedColumns.Count < 1
                    || String.IsNullOrEmpty(this.SourceSchemaName) || String.IsNullOrEmpty(this.SourceTableName)
                    || String.IsNullOrEmpty(this.DestinationSchemaName) || String.IsNullOrEmpty(this.DestinationTableName)
               )
            {
                isValid = false;
            }

            return isValid;
        }

        public string GetSourceTableReferenceForFilePath()
        {
            return this.SourceSchemaName + "-" + this.SourceTableName; // "-" because . might cause issues
        }

        #endregion
    }
}