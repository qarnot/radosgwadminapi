using Newtonsoft.Json;

namespace Radosgw.AdminAPI
{
    public class Subuser
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "permissions")]
        public string Permissions { get; set; }

        public override string ToString()
        {
            return string.Format("[Subuser: UserId={0}, Permissions={1}]", Id, Permissions);
        }
    }
}
