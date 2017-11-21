using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

namespace Radosgw.AdminAPI
{
    public class RadosGWAdminConnection
    {
        private string accessKey { get; set; }
        private string secretKey { get; set; }
        private string adminPrefix { get; set; }
        private Uri host { get; set; }

        private HashSet<string> subresourcesS3 = new HashSet<string>{ "acl",
            "lifecycle",
            "location",
            "logging",
            "notification",
            "partNumber",
            "policy",
            "requestPayment",
            "torrent",
            "uploadId",
            "uploads",
            "versionId",
            "versioning",
            "versions",
            "website",
        };

        private string CanonicalizeResource(string resource)
        {
            if (resource.Contains("?"))
            {
                var ret = new StringBuilder();
                var ioqm = resource.IndexOf('?');
                ret.Append(resource.Substring(0, ioqm));
                var subresources = resource.Substring(ioqm + 1).Split('&');
                var first = true;
                foreach (var item in subresources)
                {
                    var key = item.Split('=')[0];
                    if (subresourcesS3.Contains(key))
                    {
                        if (first)
                        {
                            first = false;
                            ret.Append("?");
                        }
                        else {
                            ret.Append("&");
                        }
                        ret.Append(WebUtility.UrlEncode(item));
                    }
                }
                return ret.ToString();
            }
            else {
                return resource;
            }
        }

        public string SendRequest(string httpVerb, string path, Dictionary<string, string> requestParameters = null, TimeSpan? timeout = null)
        {
            string HttpVerb = httpVerb;
            if (!path.StartsWith(this.adminPrefix))
                path = adminPrefix + path;
            string ContentMD5 = "";
            string ContentType = "";

            var queryparams = "";
            if (requestParameters != null && requestParameters.Count > 0)
            {
                if (path.Contains("?"))
                    queryparams = "&";
                else
                    queryparams = "?";
                queryparams += string.Join("&", requestParameters.Select(kvp =>
                    string.Format("{0}={1}", kvp.Key, Uri.EscapeDataString(kvp.Value))));
                queryparams += "&format=json";
            }
            else {
                queryparams = "?format=json";
            }


            string httpDate = DateTime.UtcNow.ToString("ddd, dd MMM yyyy HH:mm:ss +0000\n");
            var CPath = CanonicalizeResource(path + queryparams);

            string canonicalString = HttpVerb + "\n" +
                ContentMD5 + "\n" +
                ContentType + "\n" +
                "\n" +
                "x-amz-date:" + httpDate +
                CPath;

            // now encode the canonical string
            Encoding ae = new UTF8Encoding();
            // create a hashing object
            HMACSHA1 signature = new HMACSHA1();
            // secretId is the hash key
            signature.Key = ae.GetBytes(this.secretKey);
            byte[] bytes = ae.GetBytes(canonicalString);
            byte[] moreBytes = signature.ComputeHash(bytes);
            // convert the hash byte array into a base64 encoding
            string encodedCanonical = Convert.ToBase64String(moreBytes);


            var u = new Uri(host, path + queryparams);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(u);
            request.Headers.Add("x-amz-date", httpDate);
            request.Headers.Add("Authorization", "AWS " + this.accessKey + ":" + encodedCanonical);
            request.Method = HttpVerb;
            if (timeout != null) // default timeout if none specified, no timeout: TimeSpan(-1)
                request.Timeout = timeout.Value.Ticks < 0 ? -1 : (int)timeout.Value.TotalMilliseconds;

            // Get the response
            //HttpWebResponse response = null;
            try
            {
                var response = (HttpWebResponse)request.GetResponse();
                string responseString = "";

                using (Stream stream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                    responseString = reader.ReadToEnd();
                }
                response.Close();
                return responseString;

            }
            catch (WebException ex)
            {
                if (ex.Status == WebExceptionStatus.Timeout)
                    throw new TimeoutException();
                string responseString = "";
                var response = ex.Response as HttpWebResponse;
                using (Stream stream = ex.Response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                    responseString = reader.ReadToEnd();
                }

                response.Close();

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new KeyNotFoundException(responseString);
                }
                else if (response.StatusCode == HttpStatusCode.Forbidden)
                {
                    throw new UnauthorizedAccessException(responseString);
                }
                throw new Exception(responseString);
            }
        }

        private string UserWithTenant(string user, string tenant=null)
        {
            if (string.IsNullOrEmpty(tenant))
                return user;
            else
                return string.Format("{0}${1}", tenant, user);
        }

        public RadosGWAdminConnection(string host, string accessKey, string secretKey, string adminPrefix="/admin")
            : this(new Uri(host), accessKey, secretKey, adminPrefix)
        {
        }

        public RadosGWAdminConnection(Uri host, string accessKey, string secretKey, string adminPrefix="/admin")
        {
            this.host = host;
            this.secretKey = secretKey;
            this.accessKey = accessKey;
            if (!adminPrefix.StartsWith("/"))
                adminPrefix = "/" + adminPrefix;
            this.adminPrefix = adminPrefix;
        }

        public User GetUserInfo(string uid, string tenant = null, TimeSpan? timeout = null)
        {

            var parameters = new Dictionary<string, string>();
            parameters.Add("uid", UserWithTenant(uid, tenant));
            var rets = SendRequest("GET", "/user", parameters, timeout);

            return JsonConvert.DeserializeObject<User>(rets);
        }

        private User UserRequest(string reqType, string uid, string displayName, string tenant=null, 
                                 string email=null, string keyType=null, string accessKey=null,
                                 string secretKey=null, string userCaps=null, bool generateKey=true, 
                                 uint MaxBuckets=1000, bool suspended=false, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();

            req.Add("uid", UserWithTenant(uid, tenant));
            req.Add("display-name", displayName);

            if (!string.IsNullOrEmpty(email))
                req.Add("email", email);
            if (!string.IsNullOrEmpty(keyType))
                req.Add("key-type", keyType);
            if (!string.IsNullOrEmpty(accessKey))
                req.Add("access-key", accessKey);
            if (!string.IsNullOrEmpty(secretKey))
                req.Add("secret-key", secretKey);
            if (!string.IsNullOrEmpty(userCaps))
                req.Add("caps", userCaps);
            if (!generateKey)
                req.Add("generate-key", "False");
            if (MaxBuckets != 1000)
                req.Add("max-buckets", MaxBuckets.ToString());
            if (suspended)
                req.Add("suspended", "True");
            var rets = SendRequest(reqType, "/user", req, timeout);
            return JsonConvert.DeserializeObject<User>(rets);
        }

        public User CreateUser(string uid, string displayName, string tenant = null, string email = null,
                                 string keyType = null, string accessKey = null, string secretKey = null,
                                 string userCaps = null, bool generateKey = true, uint MaxBuckets = 1000,
                                 bool suspended = false)
        {
            return UserRequest("PUT", uid, displayName, tenant, email, keyType, accessKey, secretKey,
                               userCaps, generateKey, MaxBuckets, suspended);
        }

        public User ModifyUser(User u)
        {
            return ModifyUser(u.UserId, displayName: u.DisplayName, tenant: u.Tenant, email: u.Email,
                              MaxBuckets: u.MaxBuckets, suspended: u.Suspended);
        }

        public User ModifyUser(string uid, string displayName=null, string tenant=null, string email=null,
                                 string keyType=null, string accessKey=null, string secretKey=null,
                                 string userCaps=null, bool generateKey=true, uint MaxBuckets=1000,
                                 bool suspended=false)
        {
            return UserRequest("POST", uid, displayName, tenant, email, keyType, accessKey, secretKey,
                   userCaps, generateKey, MaxBuckets, suspended);
        }

        public string RemoveUser(string uid, string tenant = null, bool purgeData = false, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();
            req.Add("uid", UserWithTenant(uid, tenant));
            if (purgeData)
                req.Add("purge-data", "True");
            return SendRequest("DELETE", "/user", req, timeout);

        }

        public IList<Key> CreateKey(string uid, string tenant=null, string subuser=null, string keyType=null,
                                string accessKey=null, string secretKey=null, bool generateKey=true, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();
            req.Add("uid", UserWithTenant(uid, tenant));
            if (!string.IsNullOrEmpty(subuser))
                req.Add("subuser", subuser);
            if (!string.IsNullOrEmpty(keyType))
                req.Add("key-type", keyType);
            if (!string.IsNullOrEmpty(accessKey))
                req.Add("access-key", accessKey);
            if (!string.IsNullOrEmpty(secretKey))
                req.Add("secret-key", secretKey);
            if (!generateKey)
                req.Add("generate-key", "False");

            var rets = SendRequest("PUT", "/user?key", req, timeout);
            return JsonConvert.DeserializeObject<IList<Key>>(rets);
        }

        public string RemoveKey(string uid, string accessKey, string tenant=null, string subUser=null,
                                string keyType=null, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();
            req.Add("uid", UserWithTenant(uid, tenant));
            req.Add("access-key", accessKey);
            if (!string.IsNullOrEmpty(subUser))
                req.Add("subuser", subUser);
            if (!string.IsNullOrEmpty(keyType))
                req.Add("key-type", keyType);

            return SendRequest("DELETE", "/user?key", req, timeout);
        }

    }
}
