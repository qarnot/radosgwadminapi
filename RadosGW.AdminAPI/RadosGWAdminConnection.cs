using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
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

        public async Task<string> SendRequestAsync(string httpVerb, string path, Dictionary<string, string> requestParameters = null, TimeSpan? timeout = null)
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
            try
            {
                var response = (HttpWebResponse) await request.GetResponseAsync();
                string responseString = "";

                using (Stream stream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                    responseString = await reader.ReadToEndAsync();
                }
                response.Close();
                return responseString;

            }
            catch (WebException ex)
            {
                if (ex.Status == WebExceptionStatus.ConnectFailure)
                    throw;
                if (ex.Status == WebExceptionStatus.Timeout)
                    throw new TimeoutException();
                string responseString = "";
                using (var response = ex.Response as HttpWebResponse)
                {
                    using (Stream stream = response.GetResponseStream())
                    using(var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        responseString = await reader.ReadToEndAsync();
                    }
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
        }

        [Obsolete("SendRequest is obsolete. Consider using the async version SendRequest")]
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
                if (ex.Status == WebExceptionStatus.ConnectFailure)
                    throw;
                if (ex.Status == WebExceptionStatus.Timeout)
                    throw new TimeoutException();
                string responseString = "";
                using (var response = ex.Response as HttpWebResponse)
                {
                    using (Stream stream = response.GetResponseStream())
                    using(var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        responseString = reader.ReadToEnd();
                    }
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

        public async Task<User> GetUserInfoAsync(string uid, string tenant = null, bool includeStats = false, TimeSpan? timeout = null)
        {

            var parameters = new Dictionary<string, string>();
            parameters.Add("uid", UserWithTenant(uid, tenant));
            if (includeStats)
                parameters.Add("stats", "true");

            string rets;
            try {
                rets = await SendRequestAsync("GET", "/user", parameters, timeout);
            } catch (KeyNotFoundException) { // rgw responds with a 404 for users with no stats
                if (!includeStats)
                    throw;

                parameters.Remove("stats");
                rets = await SendRequestAsync("GET", "/user", parameters, timeout);
            }

            return JsonConvert.DeserializeObject<User>(rets);
        }

        [Obsolete("GetUserInfo is obsolete. Consider using the async version GetUserInfoAsync")]
        public User GetUserInfo(string uid, string tenant = null, bool includeStats = false, TimeSpan? timeout = null)
        {

            var parameters = new Dictionary<string, string>();
            parameters.Add("uid", UserWithTenant(uid, tenant));
            if (includeStats)
                parameters.Add("stats", "true");

            string rets;
            try {
                rets = SendRequest("GET", "/user", parameters, timeout);
            } catch (KeyNotFoundException) { // rgw responds with a 404 for users with no stats
                if (!includeStats)
                    throw;

                parameters.Remove("stats");
                rets = SendRequest("GET", "/user", parameters, timeout);
            }

            return JsonConvert.DeserializeObject<User>(rets);
        }

        public async Task<UserQuota> GetUserQuotaAsync(string uid, string tenant = null, QuotaType quotaType = QuotaType.User, TimeSpan? timeout = null)
        {
            var parameters = new Dictionary<string, string>
            {
                {"uid", UserWithTenant(uid, tenant)},
                {"quota-type", quotaType.ToString().ToLower()},
            };

            var rets = await SendRequestAsync("GET", "/user?quota", parameters, timeout);
            return JsonConvert.DeserializeObject<UserQuota>(rets);
        }

        [Obsolete("GetUserQuota is obsolete. Consider using the async version GetUserQuotaAsync")]
        public UserQuota GetUserQuota(string uid, string tenant = null, QuotaType quotaType = QuotaType.User, TimeSpan? timeout = null)
        {
            var parameters = new Dictionary<string, string>
            {
                {"uid", UserWithTenant(uid, tenant)},
                {"quota-type", quotaType.ToString().ToLower()},
            };

            var rets = SendRequest("GET", "/user?quota", parameters, timeout);
            return JsonConvert.DeserializeObject<UserQuota>(rets);
        }

        private async Task<User> UserRequestAsync(string reqType, string uid, string displayName, string tenant=null, 
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
            var rets = await SendRequestAsync(reqType, "/user", req, timeout);
            return JsonConvert.DeserializeObject<User>(rets);
        }

        [Obsolete("UserRequest is obsolete. Consider using the async version UserRequestAsync")]
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

        public async Task<User> CreateUserAsync(string uid, string displayName, string tenant = null, string email = null,
                                 string keyType = null, string accessKey = null, string secretKey = null,
                                 string userCaps = null, bool generateKey = true, uint MaxBuckets = 1000,
                                 bool suspended = false)
        {
            return await UserRequestAsync("PUT", uid, displayName, tenant, email, keyType, accessKey, secretKey,
                               userCaps, generateKey, MaxBuckets, suspended);
        }

        [Obsolete("CreateUser is obsolete. Consider using the async version CreateUserAsync")]
        public User CreateUser(string uid, string displayName, string tenant = null, string email = null,
                                 string keyType = null, string accessKey = null, string secretKey = null,
                                 string userCaps = null, bool generateKey = true, uint MaxBuckets = 1000,
                                 bool suspended = false)
        {
            return UserRequest("PUT", uid, displayName, tenant, email, keyType, accessKey, secretKey,
                               userCaps, generateKey, MaxBuckets, suspended);
        }

        public async Task<User> ModifyUserAsync(User u)
        {
            return await ModifyUserAsync(u.UserId, displayName: u.DisplayName, tenant: u.Tenant, email: u.Email,
                              MaxBuckets: u.MaxBuckets, suspended: u.Suspended);
        }

        [Obsolete("ModifyUser is obsolete. Consider using the async version ModifyUserAsync")]
        public User ModifyUser(User u)
        {
            return ModifyUser(u.UserId, displayName: u.DisplayName, tenant: u.Tenant, email: u.Email,
                              MaxBuckets: u.MaxBuckets, suspended: u.Suspended);
        }

        public async Task<User> ModifyUserAsync(string uid, string displayName=null, string tenant=null, string email=null,
                                 string keyType=null, string accessKey=null, string secretKey=null,
                                 string userCaps=null, bool generateKey=true, uint MaxBuckets=1000,
                                 bool suspended=false)
        {
            return await UserRequestAsync("POST", uid, displayName, tenant, email, keyType, accessKey, secretKey,
                   userCaps, generateKey, MaxBuckets, suspended);
        }

        [Obsolete("ModifyUser is obsolete. Consider using the async version ModifyUserAsync")]
        public User ModifyUser(string uid, string displayName=null, string tenant=null, string email=null,
                                 string keyType=null, string accessKey=null, string secretKey=null,
                                 string userCaps=null, bool generateKey=true, uint MaxBuckets=1000,
                                 bool suspended=false)
        {
            return UserRequest("POST", uid, displayName, tenant, email, keyType, accessKey, secretKey,
                   userCaps, generateKey, MaxBuckets, suspended);
        }

        public async Task<string> RemoveUserAsync(string uid, string tenant = null, bool purgeData = false, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();
            req.Add("uid", UserWithTenant(uid, tenant));
            if (purgeData)
                req.Add("purge-data", "True");
            return await SendRequestAsync("DELETE", "/user", req, timeout);
        }

        [Obsolete("RemoveUser is obsolete. Consider using the async version RemoveUserAsync")]
        public string RemoveUser(string uid, string tenant = null, bool purgeData = false, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();
            req.Add("uid", UserWithTenant(uid, tenant));
            if (purgeData)
                req.Add("purge-data", "True");
            return SendRequest("DELETE", "/user", req, timeout);

        }

        public async Task<IList<Key>> CreateKeyAsync(string uid, string tenant=null, string subuser=null, string keyType=null,
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

            var rets = await SendRequestAsync("PUT", "/user?key", req, timeout);
            return JsonConvert.DeserializeObject<IList<Key>>(rets);
        }

        [Obsolete("CreateKey is obsolete. Consider using the async version CreateKeyAsync")]
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

        public async Task<string> RemoveKeyAsync(string uid, string accessKey, string tenant=null, string subUser=null,
                                string keyType=null, TimeSpan? timeout = null)
        {
            var req = new Dictionary<string, string>();
            req.Add("uid", UserWithTenant(uid, tenant));
            req.Add("access-key", accessKey);
            if (!string.IsNullOrEmpty(subUser))
                req.Add("subuser", subUser);
            if (!string.IsNullOrEmpty(keyType))
                req.Add("key-type", keyType);

            return await SendRequestAsync("DELETE", "/user?key", req, timeout);
        }

        [Obsolete("RemoveKey is obsolete. Consider using the async version RemoveKeyAsync")]
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
