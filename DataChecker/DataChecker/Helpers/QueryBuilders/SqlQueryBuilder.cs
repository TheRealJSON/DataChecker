using System;
using System.Collections.Generic;

namespace DataCheckerProj.Helpers
{
    /// <summary>
    ///     Class used to construct SQL queries in the correct syntax.
    /// </summary>
    public abstract class SqlQueryBuilder
    {
        #region properties

        protected string SelectQuery { get; set; }
        protected string IdentifierStart { get; set; }  // the character(s) used to identify the start of a literal object reference
        protected string IdentifierEnd { get; set; }    // the character(s) used to identify the end of a literal object reference

        #endregion

        #region constructors

        public SqlQueryBuilder()
        {
            this.SelectQuery = "";
        }

        #endregion

        #region methods

        /// <summary>
        ///    Construct the SQL SELECT FROM clause for the current query, by selecting all columns of the specified SQL table.
        /// </summary>
        /// <param name="table">The SQL table that all columns should be selected/queried from.</param>
        /// <returns>Current Class instance.</returns>
        public SqlQueryBuilder SelectAllFrom(TableReference table)
        {
            this.SelectQuery = "SELECT *";
            From(table);

            return this;
        }

        /// <summary>
        ///    Construct the SQL SELECT clause for the current query, by selecting only specified columns.
        /// </summary>
        /// <param name="columns">The columns to include in the SQL SELECT clause.</param>
        /// <param name="distinct">Whether or not to only select distinct (unique) values.</param>
        /// <returns>Current Class instance.</returns>
        public SqlQueryBuilder Select(List<string> columns, bool distinct = false)
        {
            this.SelectQuery = "SELECT ";

            if (distinct)
                this.SelectQuery += "DISTINCT ";

            this.SelectQuery += string.Join(",", columns);

            return this;
        }

        /// <summary>
        ///    Construct the SQL FROM clause for the current query.
        /// </summary>
        /// <param name="table">The SQL table to be queried.</param>
        /// <returns>Current Class instance.</returns>
        public SqlQueryBuilder From(TableReference table)
        {
            this.SelectQuery += " FROM " + this.IdentifierStart + table.DatabaseName + this.IdentifierEnd
                                + "." + this.IdentifierStart + table.SchemaName + this.IdentifierEnd
                                + "." + this.IdentifierStart + table.TableName + this.IdentifierEnd;

            return this;
        }

        /// <summary>
        ///    Construct the SQL WHERE clause for the current query.
        /// </summary>
        /// <param name="whereCondition">The SQL condition to applied to the current query.</param>
        /// <returns>Current Class instance.</returns>
        public SqlQueryBuilder Where(Condition whereCondition)
        {
            string joiningClause = " WHERE ";
            if (this.SelectQuery.Contains("WHERE")) // this is not good way of doing it. What is Schema is called "WHERE"? CHANGE.
                joiningClause = " AND ";

            this.SelectQuery = joiningClause + " " 
                                + this.IdentifierStart + whereCondition.ColumnName + this.IdentifierEnd
                                + " " + whereCondition.Operation 
                                + " " + TranslateValueToSQL(whereCondition.Value);

            return this;
        }

        /// <summary>
        ///    Construct the SQL ORDER BY clause for the current query.
        /// </summary>
        /// <param name="columnsToOrderBy">The list of columns to order the current query results by.</param>
        /// <returns>Current Class instance.</returns>
        public SqlQueryBuilder OrderBy(List<string> columnsToOrderBy)
        {
            string orderByClause = " ORDER BY ";
            foreach (string columnToOrderBy in columnsToOrderBy)
            {
                orderByClause += this.IdentifierStart + columnToOrderBy + this.IdentifierEnd + ",";
            }

            orderByClause = orderByClause.Substring(0, orderByClause.Length - 1); // remove trailing comma

            this.SelectQuery += orderByClause;

            return this;
        }

        /// <summary>
        ///    Convert a value to a syntax that can be parsed in a SQL query.
        /// </summary>
        /// <param name="value">The value to be parsed in a sql query.</param>
        /// <returns>String containing the provided value in a syntax that can be parsed in a SQL query.</returns>
        public string TranslateValueToSQL(dynamic value)
        {
            // would use switch case, but not easy to do prior to c# 7 apparently
            if (value.GetType() == typeof(DateTime))
            {
                return "'" + Convert.ToDateTime(value).ToString("yyy-MM-dd HH:mm:ss.ffffff") + "'";
            }
            else if (value.GetType() == typeof(DateTimeOffset))
            {
                return "'" + value.ToString("yyy-MM-dd HH:mm:ss.ffffff") + "'";
            }
            else if (value.GetType() == typeof(string))
            {
                string strValue = Convert.ToString(value);

                if (strValue.Contains("SELECT") && strValue.Contains("FROM")) // if value is a (sub) query
                    return "(" + strValue + ")";

                return "'" + strValue + "'";
            }

            return Convert.ToString(value);
        }

        /// <summary>
        ///    Returns the SQL query that has been constructed over the lifetime of the current object by 
        ///    successive method calls.
        /// </summary>
        /// <returns>SQL query constructed over the lifetime of the current object</returns>
        public string GetSelectQuery()
        {
            return this.SelectQuery;
        }

        #endregion

        /// <summary>
        ///    A class used to conceptually represent a SQL table.
        /// </summary>
        public class TableReference
        {
            public string DatabaseName { get; set; }
            public string SchemaName { get; set; }
            public string TableName { get; set; }
            public TableReference(string database, string schema, string table)
            {
                this.DatabaseName = database;
                this.SchemaName = schema;
                this.TableName = table;
            }

            public override string ToString()
            {
                return DatabaseName + "." + SchemaName + "." + TableName;
            }
        }

        /// <summary>
        ///    A class used to conceptually represent a SQL WHERE condition.
        /// </summary>
        public class Condition
        {
            public string ColumnName { get; set; }
            public string Operation { get; set; }
            public dynamic Value { get; set; }

            public Condition(string column, string operation, dynamic value)
            {
                this.ColumnName = column;
                this.Operation = operation;
                this.Value = value;
            }
        }
    }
}