using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Collections;
using System;
using System.Diagnostics;

namespace DataCheckerProj.Helpers
{
    /// <summary>
    /// Class that provides generic methods for solving common problems
    /// related to the manipulation of <c>DataTable</c> objects.
    /// </summary>
    static class DataTableHelper
    {
        /// <summary>
        /// Compares provided DataTables and searches for records in the provided <c>sourceDataset</c> that are not in the provided <c>destinationDataset</c>
        /// </summary>
        /// <param name="sourceDataset">The "Left" DataTable.</param>
        /// <param name="destinationDataset">The "Right" DataTable.</param>
        /// <param name="comparer">A <c>DataRow</c> comparer that provides logic deciding if rows match, which is essential for deciding if a row is in both DataTables or not.</param>
        /// <returns>A <c>DataTable</c> containing <c>DataRows</c> found in the provided sourceDataset but not the provided destinationDataset.</returns>
        public static DataTable GetDataTablesLeftDisjoint(DataTable sourceDataset, DataTable destinationDataset, IEqualityComparer<DataRow> comparer)
        {
            var dt1Rows = sourceDataset.AsEnumerable();
            var dt2Rows = destinationDataset.AsEnumerable();

            // get the rows that are in dt1 but not dt2:
            // comparer object provides logic for deciding if rows match, which is essential for deciding if a row is in dt1 but not dt2
            var diffRows = dt1Rows.Except(dt2Rows, comparer);

            DataTable missingRows = sourceDataset.Clone();  // create a new DataTable with same Column-schema

            foreach (DataRow diffRow in diffRows)
            {
                DataRow rowCopy = missingRows.NewRow(); // new empty row
                rowCopy.ItemArray = diffRow.ItemArray;  // copy values
                missingRows.Rows.Add(rowCopy);
            }

            return missingRows;
        }

        /// <summary>
        ///   This method returns a IEnumerable of Datarows.
        /// </summary>
        /// <param name="source">
        ///   The source DataTable to make enumerable.
        /// </param>
        /// <returns>
        ///   IEnumerable of datarows.
        /// </returns>
        public static EnumerableRowCollection<DataRow> AsEnumerable(this DataTable source)
        {
            return new EnumerableRowCollection<DataRow>(source);
        }
    }

    /// <summary>
    /// Provides an entry point so that Cast operator call can be intercepted within an extension method.
    /// </summary>
    public abstract class EnumerableRowCollection : IEnumerable
    {
        internal abstract Type ElementType { get; }
        internal abstract DataTable Table { get; }

        internal EnumerableRowCollection()
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return null;
        }
    }

    /// <summary>
    /// This class provides a wrapper for DataTables to allow for querying via LINQ.
    /// </summary>
    public class EnumerableRowCollection<TRow> : EnumerableRowCollection, IEnumerable<TRow>
    {
        private readonly DataTable _table;
        private readonly IEnumerable<TRow> _enumerableRows;
        private readonly List<Func<TRow, bool>> _listOfPredicates;
        private readonly Func<TRow, TRow> _selector;

        #region Properties

        internal override Type ElementType
        {
            get
            {
                return typeof(TRow);
            }

        }

        internal IEnumerable<TRow> EnumerableRows
        {
            get
            {
                return _enumerableRows;
            }
        }

        internal override DataTable Table
        {
            get
            {
                return _table;
            }
        }


        #endregion Properties

        #region Constructors

        /// <summary>
        /// Basic Constructor
        /// </summary>
        internal EnumerableRowCollection(DataTable table)
        {
            _table = table;
            _enumerableRows = table.Rows.Cast<TRow>();
            _listOfPredicates = new List<Func<TRow, bool>>();
        }

        /// <summary>
        /// Copy Constructor that sets the input IEnumerable as enumerableRows
        /// Used to maintain IEnumerable that has linq operators executed in the same order as the user
        /// </summary>
        internal EnumerableRowCollection(EnumerableRowCollection<TRow> source, IEnumerable<TRow> enumerableRows, Func<TRow, TRow> selector)
        {
            Debug.Assert(null != enumerableRows, "null enumerableRows");

            _enumerableRows = enumerableRows;
            _selector = selector;
            if (null != source)
            {
                if (null == source._selector)
                {
                    _table = source._table;
                }
                _listOfPredicates = new List<Func<TRow, bool>>(source._listOfPredicates);
            }
            else
            {
                _listOfPredicates = new List<Func<TRow, bool>>();
            }
        }

        #endregion Constructors

        #region PublicInterface
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        ///  This method returns an strongly typed iterator
        ///  for the underlying DataRow collection.
        /// </summary>
        /// <returns>
        ///   A strongly typed iterator.
        /// </returns>
        public IEnumerator<TRow> GetEnumerator()
        {
            return _enumerableRows.GetEnumerator();
        }
        #endregion PublicInterface
    }
}