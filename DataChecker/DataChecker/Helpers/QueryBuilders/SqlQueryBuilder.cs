using System;
using System.Collections.Generic;

namespace DataCheckerProj.Helpers
{
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

        public SqlQueryBuilder SelectAllFrom(TableReference table)
        {
            this.SelectQuery = "SELECT *";
            From(table);

            return this;
        }

        public SqlQueryBuilder Select(List<string> columns, bool distinct = false)
        {
            this.SelectQuery = "SELECT ";

            if (distinct)
                this.SelectQuery += "DISTINCT ";

            this.SelectQuery += string.Join(",", columns);

            return this;
        }

        public SqlQueryBuilder From(TableReference table)
        {
            this.SelectQuery += " FROM " + this.IdentifierStart + table.DatabaseName + this.IdentifierEnd
                                + "." + this.IdentifierStart + table.SchemaName + this.IdentifierEnd
                                + "." + this.IdentifierStart + table.TableName + this.IdentifierEnd;

            return this;
        }

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

        public string GetSelectQuery()
        {
            return this.SelectQuery;
        }

        #endregion

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