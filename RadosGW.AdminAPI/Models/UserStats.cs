using Newtonsoft.Json;

namespace Radosgw.AdminAPI
{
    public class UserStats
    {
        [JsonProperty(PropertyName = "size")]
        public uint Size { get; set; }

        /// <summary>
        /// Size including replicas.
        /// </summary>
        [JsonProperty(PropertyName = "size_actual")]
        public uint ActualSize { get; set; }

        /// <summary>
        /// Size after compression and encryption.
        /// </summary>
        [JsonProperty(PropertyName = "size_utilized")]
        public uint SizeUtilized { get; set; }

        [JsonProperty(PropertyName = "size_kb")]
        public uint SizeKB { get; set; }

        /// <summary>
        /// Size including replicas, rounded to KB.
        /// </summary>
        [JsonProperty(PropertyName = "size_kb_actual")]
        public uint ActualSizeKB { get; set; }

        /// <summary>
        /// Size after compression and encryption, rounded to KB.
        /// </summary>
        [JsonProperty(PropertyName = "size_kb_utilized")]
        public uint SizeUtilzedKB { get; set; }

        [JsonProperty(PropertyName="num_objects")]
        public uint NumObjects { get; set; }

        public override string ToString()
        {
            return $"[UserStats: Size={Size}, SizeUtilized={SizeUtilized}, ActualSize={ActualSize}]";
        }

        public UserStats()
        {}
    }
}
