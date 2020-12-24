using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DataCheckerProj.Mapping
{
    public class TableMapping : ICloneable
    {
        #region properties

        public string SourceSchemaName { get; set; }
        public string SourceTableName { get; set; }
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

        /// <summary>
        /// Used to load table/column mappings from a database.
        /// </summary>
        /// <param name="schemaName">The name of the source schema for which to load mappings</param>
        /// <param name="tableName">The name of the source table for which to load mappings</param>
        public void LoadMappingFromSqlServerDatabase(string connString, string schemaName, string tableName, string columnMappingsSource, string tableMappingsSource)
        {
            this.SourceSchemaName = schemaName;
            this.SourceTableName = tableName;
            this.DestinationTableName = tableName;

            string sqlQuery = string.Format("SELECT * FROM [" + schemaName + "].[" + tableName + "]", columnMappingsSource, tableMappingsSource, schemaName, tableName);

            using (SqlConnection conn = new SqlConnection(connString))
            {
                using (SqlDataAdapter da = new SqlDataAdapter(sqlQuery, conn))
                {
                    DataTable mappings = new DataTable();
                    da.Fill(mappings);

                    foreach (DataRow row in mappings.Rows)
                    {
                        this.DestinationSchemaName = row["destination_schema"].ToString(); // going to do these operations each row = unecessary TODO: refactor
                        this.DestinationTableName = row["destination_table"].ToString(); // going to do these operations each row = unecessary TODO: refactor

                        MappedColumns.Add(new ColumnMapping(row["source_column"].ToString(),
                                                            row["source_type"].ToString(),
                                                            row["destination_column"].ToString(),
                                                            row["destination_type"].ToString()));

                    }
                }
            }
        }

        public ColumnMapping GetColumnMappingBySourceCol(string sourceColName)
        {
            foreach (ColumnMapping col in this.MappedColumns)
            {
                if (col.SourceColumnName.Equals(sourceColName))
                {
                    return col;
                }
            }

            return null;
        }

        public List<ColumnMapping> GetColumnMappingListBySourceCols(List<string> sourceColNames)
        {
            List<ColumnMapping> subset = new List<ColumnMapping>();
            foreach (ColumnMapping col in this.MappedColumns)
            {
                var match = sourceColNames.Find(x => x == col.SourceColumnName);
                if (match != null)
                {
                    subset.Add(col);
                }
            }
            return subset;
        }

        public List<ColumnMapping> GetDistinctColumns()
        {
            List<ColumnMapping> distinctCols = new List<ColumnMapping>();
            foreach (ColumnMapping col in this.MappedColumns)
            {
                if (distinctCols.Find(c => c.DestinationColumnName.Equals(col.DestinationColumnName)) == null) // if destination col doesn't already exist
                    distinctCols.Add(col);
            }
            return distinctCols;
        }

        public List<ColumnMapping> GetIdentityColumns()
        {
            List<ColumnMapping> identityCols = new List<ColumnMapping>();
            foreach (ColumnMapping col in this.MappedColumns)
            {
                if (col.IsIdentityColumn)
                    identityCols.Add(col);
            }
            return identityCols;
        }

        public bool Validate()
        {
            bool isValid = true;

            List<string> usedDestCols = new List<string>();
            foreach (ColumnMapping colMap in this.MappedColumns)
            {
                /* Check for duplicate columns */
                if (usedDestCols.Contains(colMap.DestinationColumnName))
                    isValid = false;
                usedDestCols.Add(colMap.DestinationColumnName);
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

        #endregion

        public object Clone()
        {
            TableMapping clone = new TableMapping();
            clone.SourceSchemaName = this.SourceSchemaName;
            clone.SourceTableName = this.SourceTableName;
            clone.DestinationSchemaName = this.DestinationSchemaName;
            clone.DestinationTableName = this.DestinationTableName;
            clone.MappedColumns = this.MappedColumns;

            return clone;
        }
    }
}
