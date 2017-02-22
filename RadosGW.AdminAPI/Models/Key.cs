using System;
using Newtonsoft.Json;

namespace Radosgw.AdminAPI
{
    public class Key
    {
        [JsonProperty(PropertyName = "access_key")]
        public string AccessKey { get; set; }

        [JsonProperty(PropertyName = "secret_key")]
        public string SecretKey { get; set; }

        [JsonProperty(PropertyName = "user")]
        public string UserWithTenant { get; set; }

        public Key()
        {
        }

        public override string ToString()
        {
            return string.Format("[Key: AccessKey={0}, SecretKey={1}, UserWithTenant={2}]", AccessKey, SecretKey, UserWithTenant);
        }
    }
}
