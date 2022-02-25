using System;
using System.Runtime.Serialization;
using WindwardModels;
using WindwardRepository;

namespace SampleRESTfulS3Plugin
{
    public enum JobRequestAction
    {
        CREATE,
        UPDATE_STATUS,
        GET_STATUS,
        GET_DOCUMENT,
        DELETE
    }

    [DataContract]
    public class JobRequestData
    {
        [DataMember]
        public Template Template { get; set; }

        [DataMember]
        public RepositoryStatus.REQUEST_TYPE RequestType { get; set; }

        [DataMember]
        public DateTime CreationDate { get; set; }
    }
}
