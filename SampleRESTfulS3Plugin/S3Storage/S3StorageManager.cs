using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using WindwardModels;
using WindwardRepository;
using Metrics = WindwardModels.Metrics;

namespace SampleRESTfulS3Plugin.S3Storage
{
    class S3StorageManager
    {
        private static AmazonDynamoDBClient Client;
        private IAmazonS3 S3Client;
        private static DynamoDBContext DynamoContext;
        private static TransferUtility TransferUtility;
        private static string BucketName;
        private static string DocumentsFolder;
        private static string TemplatesFolder;

        private static readonly ILog Log = LogManager.GetLogger(typeof(S3StorageManager));

        public S3StorageManager(BasicAWSCredentials aWSCredentials, TransferUtility transferUtility, IAmazonS3 s3Client, string bucketName)
        {
            createClient(aWSCredentials);
            DynamoContext = new DynamoDBContext(Client);
            TransferUtility = transferUtility;
            BucketName = bucketName;
            DocumentsFolder = bucketName + "/Documents";
            TemplatesFolder = bucketName + "/Templates";
            S3Client = s3Client;
        }


        public async Task<bool> AddRequest(JobRequestData request)
        {
            // Add a request to storage
            JobInfoEntity entity = JobInfoEntity.FromJobRequestData(request);
            entity.Status = (int)RepositoryStatus.JOB_STATUS.Pending;

            try
            {
                Task putTask = DynamoContext.SaveAsync(entity);
                await putTask;
                TaskStatus status = putTask.Status;
                bool success = status == TaskStatus.RanToCompletion;

                if (success)
                {
                    Log.Debug($"Added template [{request.Template.Guid}] to blob storage");
                    using (MemoryStream memStream = SerializeToStream(request.Template))
                    {
                        await TransferUtility.UploadAsync(memStream, BucketName+"/Templates", request.Template.Guid);
                    }
                    
                }
                else
                {
                    Log.Error($"Failed to add template [{request.Template.Guid}] to blob storage: {status}");

                }
                return success;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<bool> UpdateRequest(string guid, RepositoryStatus.JOB_STATUS newStatus)
        {
            JobInfoEntity entity = GetRequestInfo(guid).Result;
            entity.Status = (int)newStatus;

            Task putTask = DynamoContext.SaveAsync(entity);
            await putTask;
            TaskStatus status = putTask.Status;
            bool success = status == TaskStatus.RanToCompletion;

            if (success)
                Log.Debug($"Updated request [{guid}] status to {newStatus}");
            else
                Log.Error($"Failed to updat request [{guid}] status to {newStatus}: {status}");

            return success;

        }

        public async Task<bool> CompleteRequest<T>(string guid, T generatedEntity)
        {
            JobInfoEntity entity = GetRequestInfo(guid).Result;

            using (MemoryStream memStream = SerializeToStream(generatedEntity))
            {
                await TransferUtility.UploadAsync(memStream, DocumentsFolder, guid);
            }

            if (generatedEntity is ServiceError)
                entity.Status = (int)RepositoryStatus.JOB_STATUS.Error;
            else
                entity.Status = (int)RepositoryStatus.JOB_STATUS.Complete;

            try
            {
                await DynamoContext.SaveAsync(entity);
                Log.Info("Saved report ( " + guid + ") to blob storage.");
                return true;
            }
            catch (Exception ex)
            {

                Log.Info("Failed to save report ( " + guid + ") to blob storage. Exception: "+ex);
                return false;
            }

        }


        public async Task<bool> DeleteRequest(string guid)
        {
            try
            {
                await DynamoContext.DeleteAsync(guid);

                DeleteObjectRequest deleteTemplate = new DeleteObjectRequest
                {
                    BucketName = TemplatesFolder,
                    Key = guid,
                };
                await S3Client.DeleteObjectAsync(deleteTemplate);
                DeleteObjectRequest deleteDocument = new DeleteObjectRequest
                {
                    BucketName = DocumentsFolder,
                    Key = guid,
                };
                await S3Client.DeleteObjectAsync(deleteDocument);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<bool> RevertGeneratingJobsToPending()
        {
            bool success = true;

            try
            {
                List<ScanCondition> conditions = new List<ScanCondition>();
                conditions.Add(new ScanCondition("Status", ScanOperator.Equal, (int)RepositoryStatus.JOB_STATUS.Generating));
                List<JobInfoEntity> data = await DynamoContext.QueryAsync<JobInfoEntity>(conditions).GetRemainingAsync();
                //List <JobInfoEntity> entities = new List<JobInfoEntity>();
                foreach (JobInfoEntity item in data)
                {
                    // revert status to pending and add to list to be updated
                    //item.Status = (int)RepositoryStatus.JOB_STATUS.Pending;
                    //entities.Add(item);
                    await UpdateRequest(item.Guid, RepositoryStatus.JOB_STATUS.Pending);
                }

                return success;
            }
            catch (Exception)
            {
                return false;
            }
               
        }

        public async Task<int> DeleteOldRequests(DateTime cutoff)
        {
           
            List<ScanCondition> conditions = new List<ScanCondition>();
            conditions.Add(new ScanCondition("CreationDate", ScanOperator.LessThanOrEqual, cutoff));
            List<JobInfoEntity> data = await DynamoContext.QueryAsync<JobInfoEntity>(conditions).GetRemainingAsync();
            int count = 0;
            foreach (JobInfoEntity item in data)
            {
                // revert status to pending and add to list to be updated
                //item.Status = (int)RepositoryStatus.JOB_STATUS.Pending;
                //entities.Add(item);
                bool success = await DeleteRequest(item.Guid);
                if(success)
                    count++;
            }

            return count ;
           
        }

        public async Task<JobInfoEntity> GetRequestInfo(string guid)
        {
            //AsyncSearch<JobInfoEntity> t = await DynamoContext.QueryAsync<JobInfoEntity>(guid);
            //var req = new QueryRequest
            //{
            //    TableName = "S3Test",
            //    KeyConditionExpression = "Guid = :Guid"
            //};
            //req.ExpressionAttributeValues.Add(":Guid", new AttributeValue(guid));

            JobInfoEntity res = await DynamoContext.LoadAsync<JobInfoEntity>(guid);

            return res;
        }

        public async Task<JobRequestData> GetOldestPendingJobAndGenerate()
        {
            try
            {
                List<ScanCondition> conditions = new List<ScanCondition>();
                conditions.Add(new ScanCondition("Status", ScanOperator.Equal, (int)RepositoryStatus.JOB_STATUS.Pending));


                List<JobInfoEntity> entities;
                JobInfoEntity oldestEntity = null;

                bool fourTwelveEx = true;
                while (fourTwelveEx)
                {
                    
                    try
                    {
                        entities = await DynamoContext.ScanAsync<JobInfoEntity>(conditions).GetRemainingAsync();
                        
                        if (entities.Count == 0)
                            return null;

                        oldestEntity = entities.OrderBy(d => d.CreationDate).ToArray().FirstOrDefault();
                        

                        // Set this entity to locked so no others use it and set to generating
                        oldestEntity.Status = (int)RepositoryStatus.JOB_STATUS.Generating;
                        
                        fourTwelveEx = false;

                        Log.Info($"[S3StorageManager] Updated job entity [{oldestEntity.Guid}] to generating.");

                        // Get the template for this job
                        Template template = await GetEntityFromBlob<Template>(oldestEntity.Guid, TemplatesFolder);
                        await DynamoContext.SaveAsync(oldestEntity);
                        return new JobRequestData
                        {
                            Template = template,
                            RequestType = (RepositoryStatus.REQUEST_TYPE)oldestEntity.Type
                        };
                    }
                    catch (AmazonDynamoDBException ex)
                    {
                        if (((int)ex.StatusCode) == 412)
                        {
                            Log.Warn("[S3StorageManager] Entity has changed since it was retrieved. Trying again");
                            return null;
                        }
                        else
                        {
                            Log.Error($"[S3StorageManager] StorageException in GetOldestPendingJobAndGenerate: {ex.Message}");
                            return null;
                        }
                    }
                } 
            }
            catch (Exception e)
            {
                Log.Error($"[S3StorageManager] Exception in GetOldestPendingJobAndGenerate: {e.Message}");
                return null;
            }

            return null;
        }

        private async Task<T> GetEntityFromBlob<T>(string guid, string folderName)
        {
            try
            {
                GetObjectRequest request = new GetObjectRequest()
                {
                    BucketName = folderName,
                    Key = guid
                };
                GetObjectResponse response = await S3Client.GetObjectAsync(request);
                MemoryStream memoryStream = new MemoryStream();

                using (Stream responseStream = response.ResponseStream)
                {
                    responseStream.CopyTo(memoryStream);
                }
                T ret = (T)DeserializeFromStream(memoryStream);
                memoryStream.Dispose();
                return ret;
            }
            catch (AmazonS3Exception ex)
            {
                Log.Error("Exception for guid(" + guid + "):" + ex);
                throw ex;
            }
            
        }

        public async Task<WindwardModels.Document> GetGeneratedReport(string guid)
        {
            // get generated report from blob storage
            return await GetEntityFromBlob<WindwardModels.Document>(guid, DocumentsFolder);
        }

        public async Task<ServiceError> GetError(string guid)
        {
            // get error from blob storage
            return await GetEntityFromBlob<ServiceError>(guid, DocumentsFolder);
        }

        public async Task<Metrics> GetMetrics(string guid)
        {
            // get metrics from blob storage
            return await GetEntityFromBlob<Metrics>(guid, DocumentsFolder);
        }

        public async Task<TagTree> GetTagTree(string guid)
        {
            // get tag tree from blob storage
            return await GetEntityFromBlob<TagTree>(guid, DocumentsFolder);
        }

        public static MemoryStream SerializeToStream(object o)
        {
            MemoryStream stream = new MemoryStream();
            IFormatter formatter = new BinaryFormatter();
            formatter.Serialize(stream, o);
            return stream;
        }

        public static object DeserializeFromStream(MemoryStream stream)
        {
            IFormatter formatter = new BinaryFormatter();
            stream.Seek(0, SeekOrigin.Begin);
            object o = formatter.Deserialize(stream);
            return o;
        }

        public static void createClient(BasicAWSCredentials aWSCredentials)
        {
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            clientConfig.RegionEndpoint = RegionEndpoint.USEast1;
            Client = new AmazonDynamoDBClient(aWSCredentials, clientConfig);
        }
    }
}
