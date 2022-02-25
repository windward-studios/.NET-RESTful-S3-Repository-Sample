using Amazon.DynamoDBv2.DataModel;
using System;

namespace SampleRESTfulS3Plugin.S3Storage
{
    [DynamoDBTable("YOUR_TABLE_NAME")]
    class JobInfoEntity
    {
        [DynamoDBHashKey]
        public string Guid { get; set; }

        [DynamoDBProperty("Type")]
        public int Type { get; set; }

        [DynamoDBProperty("Status")]
        public int Status { get; set; }

        [DynamoDBProperty("CreationDate")]
        public DateTime CreationDate { get; set; }

        public static JobInfoEntity FromJobRequestData(JobRequestData data)
        {
            return new JobInfoEntity
            {
                Guid = data.Template.Guid,
                CreationDate = data.CreationDate,
                Type = (int)data.RequestType
              
            };
        }
    }
}
