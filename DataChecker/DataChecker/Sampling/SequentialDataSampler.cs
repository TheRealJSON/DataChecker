using DataCheckerProj.Helpers;
using DataCheckerProj.Importers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using static DataCheckerProj.Helpers.SqlQueryBuilder;

namespace DataCheckerProj.Sampling
{
    /// <summary>
    /// Class that implements a strategy for reading data samples from a data source
    /// in a sequential manner.
    /// </summary>
    public class SequentialDataSampler : DataSampler
    {
        #region properties

        public string SamplingQuery { get; private set; } // used for unit testing

        #endregion

        #region constructors

        public SequentialDataSampler(IDbConnection dataSourceConn, TableReference dataSource, List<string> columnsToOrderBy)
        {
            this.LastSampleTaken = null;
            this.SampleSize = 100000; // 100,000
            this.SamplingQuery = CreateSamplingQuery(dataSource, null, null, columnsToOrderBy);
            this.DataSourceReader = new PostgreSqlImporter(dataSourceConn, this.SamplingQuery);
        }

        public SequentialDataSampler(IDbConnection dataSourceConn, TableReference dataSource, List<Condition> whereConditions, List<string> columnsToOrderBy)
        {
            this.LastSampleTaken = null;
            this.SampleSize = 100000; // 100,000
            this.SamplingQuery = CreateSamplingQuery(dataSource, null, whereConditions, columnsToOrderBy);
            this.DataSourceReader = new PostgreSqlImporter(dataSourceConn, this.SamplingQuery);
        }

        #endregion

        #region method

        private string CreateSamplingQuery(TableReference table, List<string> columnsToSelect = null, List<Condition> whereConditions = null, List<string> columnsToOrderBy = null)
        {
            PostgreSqlQueryBuilder samplingQueryBuilder = new PostgreSqlQueryBuilder();

            if (columnsToSelect == null)
            {
                samplingQueryBuilder.SelectAllFrom(table);
            }
            else
            {
                samplingQueryBuilder.Select(columnsToSelect)
                                    .From(table);
            }

            if (whereConditions != null)
            {
                foreach (Condition whereCondition in whereConditions)
                {
                    samplingQueryBuilder.Where(whereCondition);
                }
            }

            if (columnsToOrderBy != null)
            {
                samplingQueryBuilder.OrderBy(columnsToOrderBy);
            }

            return samplingQueryBuilder.GetSelectQuery();
        }

        public override DataTable SampleDataSource()
        {
            this.LastSampleTaken = this.DataSourceReader.NextDataSegment(this.SampleSize); // sequential sampling is nice and easy...
            return this.LastSampleTaken;
        }

        public static SequentialDataSampler GetBoundedSequentialDataSampler(IDbConnection conn, TableReference sqlTableReference, Dictionary<string, dynamic> lowerboundRecordIdentity, Dictionary<string, dynamic> upperboundRecordIdentity, List<string> columnsToOrderBy = null)
        {
            // Verify provided record identity dictionaries have same structure
            foreach (KeyValuePair<string, dynamic> lowerboundRecordIdentityItem in lowerboundRecordIdentity) //foreach column_name to column_value mapping 
            {
                string lowerboundIdentityColumnName = lowerboundRecordIdentityItem.Key;

                bool bothRecordColumnNamesMatch = upperboundRecordIdentity.ContainsKey(lowerboundIdentityColumnName);
                bool bothRecordColumnTypesMatch = false; // default to false to be properly evaluated later if necessary

                if (bothRecordColumnNamesMatch) // can only check if types match if column exists in both dictionaries
                    bothRecordColumnTypesMatch = upperboundRecordIdentity[lowerboundIdentityColumnName].GetType() == lowerboundRecordIdentity[lowerboundIdentityColumnName].GetType();

                if (bothRecordColumnNamesMatch == false || bothRecordColumnTypesMatch == false)
                {
                    throw new ArgumentException("Provided lowerboundRecordIdentity and upperboundRecordIdentity parameters do not have the same structure (keys and value data types). " +
                                        "Both parameters should describe data elements with the same structure.");
                }
            }

            // Turn lowerbound and upperbound record identities into WHERE conditions (for SQL)
            List<Condition> boundaryConditions = new List<Condition>();
            Condition lowerboundCondition;
            Condition upperboundCondition;
            string columnName;
            dynamic lowerboundValue, upperboundValue;

            for (int i = 0; i < lowerboundRecordIdentity.Count; i++) // foreach identity column, assumption both lowerbound and upperbound identities have same structure
            {
                columnName = lowerboundRecordIdentity.ElementAt(i).Key; // current identity column

                lowerboundValue = lowerboundRecordIdentity[columnName]; // value that identity column should be >=
                lowerboundCondition = new Condition(columnName, ">=", lowerboundValue);

                upperboundValue = upperboundRecordIdentity[columnName]; // value that identity column should be <=
                upperboundCondition = new Condition(columnName, "<=", upperboundValue);

                boundaryConditions.Add(lowerboundCondition);
                boundaryConditions.Add(upperboundCondition);
            }

            return new SequentialDataSampler(conn, sqlTableReference, boundaryConditions, columnsToOrderBy);
        }

        public Type GetDataReaderType() // for testing
        {
            return this.DataSourceReader.GetType();
        }

        #endregion
    }
}
