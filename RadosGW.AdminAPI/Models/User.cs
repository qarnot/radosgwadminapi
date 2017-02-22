using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Radosgw.AdminAPI
{
    public class User
    {
        [JsonProperty(PropertyName = "display_name")]
        public string DisplayName { get; set; }

        [JsonProperty(PropertyName = "user_id")]
        public string UserId { get; set; }

        [JsonProperty(PropertyName = "tenant")]
        public string Tenant { get; set; }

        [JsonProperty(PropertyName = "email")]
        public string Email { get; set; }

        [JsonProperty(PropertyName = "max_buckets")]
        public uint MaxBuckets { get; set; }

        [JsonConverter(typeof(BoolConverter))]
        [JsonProperty(PropertyName = "suspended")]
        public bool Suspended { get; set; }

        [JsonProperty(PropertyName="keys")]
        public List<Key> Keys { get; set; }

        public override string ToString()
        {
            return string.Format("[User: DisplayName={0}, UserId={1}, Tenant={2}, Email={3}, MaxBuckets={4}, Suspended={5}, Keys={6}]", 
                                 DisplayName, UserId, Tenant, Email, MaxBuckets, Suspended, string.Format("[{0}]", string.Join(",", Keys)));
        }

        public User()
        {
        }
    }
}
