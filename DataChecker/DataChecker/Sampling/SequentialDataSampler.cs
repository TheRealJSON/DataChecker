using System.Data;

namespace DataCheckerProj.Sampling
{
    public class SequentialDataSampler : DataSampler
    {
        #region properties

        public string SamplingQuery { get; private set; } // used for unit testing

        #endregion

        #region constructors

        public SequentialDataSampler()
        {
            this.LastSampleTaken = null;
            this.SampleSize = 100000; // 100,000
            this.SamplingQuery = null;
            this.DataSourceReader = null;
        }

        #endregion

        #region method

        public override DataTable SampleDataSource()
        {
            return this.LastSampleTaken;
        }

        #endregion
    }
}