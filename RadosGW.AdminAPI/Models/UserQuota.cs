using Newtonsoft.Json;

namespace Radosgw.AdminAPI
{
    public enum QuotaType
    {
        Bucket,
        User,
    }

    public class UserQuota
    {
        [JsonConverter(typeof(BoolConverter))]
        [JsonProperty(PropertyName = "enabled")]
        public bool Enabled { get; set; }

        [JsonConverter(typeof(BoolConverter))]
        [JsonProperty(PropertyName = "check_on_raw")]
        public bool CheckOnRaw { get; set; }

        [JsonProperty(PropertyName = "max_size")]
        public long MaxSize { get; set; }

        [JsonProperty(PropertyName = "max_size_kb")]
        public long MaxSizeKB { get; set; }

        [JsonProperty(PropertyName = "max_objects")]
        public long MaxObjects { get; set; }

        public override string ToString()
        {
            return $"[UserQuota: Enabled={Enabled}, CheckOnRaw={CheckOnRaw}, MaxSize={MaxSize}, MaxObjects={MaxObjects}]";
        }

        public UserQuota()
        {}
    }
}
