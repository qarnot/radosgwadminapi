

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using NUnit;
using NUnit.Framework;
using Radosgw.AdminAPI;

namespace Radosgw.AdminAPI.Tests
{
    public class RadosGWAdminConnectionTests
    {
        private static Random Random = new Random();
        private RadosGWAdminConnection RadosGWAdminClient;
        private readonly string SubuserId = "subuser";
        private readonly string SubuserAccess = "subuserAccess";
        private string SubuserSecret = "subuserSecret";
        private User MainUser;
        private string endpoint;

        [OneTimeSetUp]
        public void Setup()
        {
            var access = Environment.GetEnvironmentVariable("RADOSGWADMIN_TESTS_ACCESS");
            var secret = Environment.GetEnvironmentVariable("RADOSGWADMIN_TESTS_SECRET");
            endpoint = Environment.GetEnvironmentVariable("RADOSGWADMIN_TESTS_ENDPOINT");
            var adminPath = Environment.GetEnvironmentVariable("RADOSGWADMIN_TESTS_PATH");
            RadosGWAdminClient = new RadosGWAdminConnection(endpoint, access, secret, adminPath);
        }

        [OneTimeTearDown]
        public async Task TearDown()
        {
            try
            {
                await RadosGWAdminClient.RemoveUserAsync(
                    MainUser.UserId,
                    MainUser.Tenant,
                    true);
            }
            catch (Exception)
            {
            }
        }

        public static string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyz";
            return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[Random.Next(s.Length)]).ToArray());
        }

        private AmazonS3Client GetMainUserClient() => GetUserClient(
            MainUser.Keys.First().AccessKey,
            MainUser.Keys.First().SecretKey);

        private AmazonS3Client GetUserClient(string access, string secret)
        {
            return new AmazonS3Client(
                access,
                secret,
                new AmazonS3Config()
                {
                    ServiceURL = endpoint,
                    UseHttp = endpoint.StartsWith("http://"),
                    ForcePathStyle = true,
                }
            );
        }

        [Test, NonParallelizable, Order(1)]
        public async Task EnsureRadosGWAdminCanCreateUsers()
        {
            MainUser = await RadosGWAdminClient.CreateUserAsync(
                "userUid",
                "userDisplayName",
                "userTenant",
                "user@mail.com",
                "s3",
                "userAccess",
                "userSecret",
                generateKey: false);

            Assert.AreEqual(MainUser.UserId, "userUid");
            Assert.AreEqual(MainUser.DisplayName, "userDisplayName");
            Assert.AreEqual(MainUser.Tenant, "userTenant");
            Assert.AreEqual(MainUser.Email, "user@mail.com");
            Assert.AreEqual(MainUser.Keys.First().AccessKey, "userAccess");
            Assert.AreEqual(MainUser.Keys.First().SecretKey, "userSecret");

            var s3Client = GetMainUserClient();
            var bucketCreationResponse = await s3Client.PutBucketAsync(RandomString(12));
            Assert.LessOrEqual((int)bucketCreationResponse.HttpStatusCode, 299);
        }

        [Test, NonParallelizable, Order(2)]
        public async Task EnsureRadosGWAdminCanModifyUsers()
        {
            var newuserDisplayName = "newUserDisplayName";
            var newMail = "usernewmail@mail.com";
            var newUserAccess = "newUserAccess";
            var newUserSecret = "newUserSecret";

            MainUser = await RadosGWAdminClient.ModifyUserAsync(
                MainUser.UserId,
                newuserDisplayName,
                MainUser.Tenant,
                newMail,
                "s3",
                newUserAccess,
                newUserSecret,
                generateKey: false);

            Assert.AreEqual(MainUser.UserId, "userUid");
            Assert.AreEqual(MainUser.DisplayName, newuserDisplayName);
            Assert.AreEqual(MainUser.Tenant, "userTenant");
            Assert.AreEqual(MainUser.Email, newMail);
            Assert.AreEqual(MainUser.Keys.First().AccessKey, newUserAccess);
            Assert.AreEqual(MainUser.Keys.First().SecretKey, newUserSecret);

            var s3Client = GetMainUserClient();
            var bucketCreationResponse = await s3Client.PutBucketAsync(RandomString(12));
            Assert.LessOrEqual((int)bucketCreationResponse.HttpStatusCode, 299);
        }

        [Test, NonParallelizable, Order(3)]
        public async Task EnsureRadosGWAdminCanDeleteUsers()
        {
            var bucket = RandomString(12);
            var s3Client = GetMainUserClient();
            var bucketCreationResponse = await s3Client.PutBucketAsync(bucket);
            Assert.LessOrEqual((int)bucketCreationResponse.HttpStatusCode, 299);
            var buckets = await s3Client.ListBucketsAsync();
            Assert.True(buckets.Buckets.Select(b => b.BucketName).Contains(bucket));

            await RadosGWAdminClient.RemoveBucketAsync(bucket, MainUser.Tenant);

            buckets = await s3Client.ListBucketsAsync();
            Assert.False(buckets.Buckets.Select(b => b.BucketName).Contains(bucket));
        }

        [Test, NonParallelizable, Order(4)]
        public async Task EnsureRadosGWAdminCanCreateSubUsers()
        {
            var subusers = await RadosGWAdminClient.CreateSubuserAsync(
                MainUser.UserId,
                MainUser.Tenant,
                SubuserId,
                "s3",
                "readwrite",
                SubuserAccess,
                SubuserSecret,
                generateSecret: false);

            // ensure sub user containet main tenant and id and subuser id
            Assert.AreEqual(subusers.First().Id, $"{MainUser.Tenant}${MainUser.UserId}:{SubuserId}");

            // ensure the sub user have write access(bucket creation)
            var successBucket = RandomString(12);
            var subuserClient = GetUserClient(SubuserAccess, SubuserSecret);
            var bucketCreationResponse = await subuserClient.PutBucketAsync(successBucket);
            Assert.LessOrEqual((int)bucketCreationResponse.HttpStatusCode, 299);

            // ensure main client can see new bucket
            var mainUserClient = GetMainUserClient();
            var buckets = await mainUserClient.ListBucketsAsync();
            Assert.True(buckets.Buckets.Select(b => b.BucketName).Contains(successBucket));
        }

        [Test, NonParallelizable, Order(5)]
        public async Task EnsureRadosGWAdminCanModifySubUserKeys()
        {
            SubuserSecret = "newsubusersecret";
            await RadosGWAdminClient.CreateKeyAsync( // create or update for existing key
                MainUser.UserId,
                MainUser.Tenant,
                SubuserId,
                "s3",
                SubuserAccess,
                SubuserSecret,
                generateKey: false);

            // ensure the sub user have write access(bucket creation)
            var successBucket = RandomString(12);
            var subuserClient = GetUserClient(SubuserAccess, SubuserSecret);
            var bucketCreationResponse = await subuserClient.PutBucketAsync(successBucket);
            Assert.LessOrEqual((int)bucketCreationResponse.HttpStatusCode, 299);

            // ensure main client can see new bucket
            var mainUserClient = GetMainUserClient();
            var buckets = await mainUserClient.ListBucketsAsync();
            Assert.True(buckets.Buckets.Select(b => b.BucketName).Contains(successBucket));
        }

        [Test, NonParallelizable, Order(6)]
        public async Task EnsureRadosGWAdminCanModifySubUsers()
        {
            await RadosGWAdminClient.ModifySubuserAsync(
                MainUser.UserId,
                SubuserId,
                MainUser.Tenant,
                "s3",
                "read");

            try
            {
                var subuserClient = GetUserClient(SubuserAccess, SubuserSecret);
                var bucketCreationResponse = await subuserClient.PutBucketAsync(RandomString(12));
                Assert.Greater((int)bucketCreationResponse.HttpStatusCode, 299);
            }
            catch (Exception)
            {
                Assert.Pass();
            }
        }

        [Test, NonParallelizable, Order(7)]
        public async Task EnsureRadosGWAdminCanDeleteSubUsers()
        {
            await RadosGWAdminClient.RemoveSubuserAsync(
                MainUser.UserId,
                MainUser.Tenant,
                SubuserId,
                purgeKeys: true);

            try
            {
                var subuserClient = GetUserClient(SubuserAccess, SubuserSecret);
                var bucketCreationResponse = await subuserClient.PutBucketAsync(RandomString(12));
                Assert.Greater((int)bucketCreationResponse.HttpStatusCode, 299);
            }
            catch (Exception)
            {
                Assert.Pass();
            }
        }


        [Test, NonParallelizable, Order(99)]
        public async Task EnsureRadosGWAdminCanRemoveUsers()
        {
            await RadosGWAdminClient.RemoveUserAsync(
                MainUser.UserId,
                MainUser.Tenant,
                true);

            try
            {
                var s3Client = GetMainUserClient();
                var bucketCreationResponse = await s3Client.PutBucketAsync(RandomString(12));
                Assert.Greater((int)bucketCreationResponse.HttpStatusCode, 299);
            }
            catch (Exception)
            {
                Assert.Pass();
            }
        }
    }
}