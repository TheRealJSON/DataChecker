using DataCheckerProj.Importers;
using System.Data;

namespace DataCheckerProj.Sampling
{
    /// <summary>
    /// Abstract class that implements the <c>IDataSamplingStrategy</c> interface and 
    /// conceptually defines a class that specifies a strategy for reading data samples from a data source
    /// </summary>
    public abstract class DataSampler : IDataSamplingStrategy
    {
        #region properties

        public int SampleSize { get; set; }

        /// <summary>
        /// Object/field used to read data from a data source
        /// </summary>
        protected SqlDataImporter DataSourceReader;

        public DataTable LastSampleTaken { get; protected set; }

        #endregion

        #region constructors

        #endregion

        #region method

        public abstract DataTable SampleDataSource();

        #endregion
    }
}