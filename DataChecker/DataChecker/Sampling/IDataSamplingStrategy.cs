using System.Data;

namespace DataCheckerProj.Sampling
{
    /// <summary>
    /// Interface defining a class that implements a strategy for sampling data from a data source (database, file etc.)
    /// </summary>
    public interface IDataSamplingStrategy
    {
        /// <summary>
        /// The number of records to include in each data sample taken using specified sampling strategy
        /// </summary>
        int SampleSize { get; set; }

        /// <summary>
        /// The last data sample taken using defined strategy
        /// </summary>
        DataTable LastSampleTaken { get; }

        /// <summary>
        /// Employs the implemented sampling strategy to retrieve a data sample from a data source
        /// and stores a copy of the results in the <c>LastSampleTaken</c> property.
        /// The number of records sampled is equal to the value of the <c>SampleSize</c> property.
        /// </summary>
        /// <returns>Data sampled from a data source using the implemented strategy. A copy of the <c>LastSampleTaken</c> property</returns>
        DataTable SampleDataSource();
    }
}