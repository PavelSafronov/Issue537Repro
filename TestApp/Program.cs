using System;
using System.Collections.Generic;
using System.Threading;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;

namespace TestApp
{
    class Program
    {
        private const int clientIdentifier = 42;
        private const string tableName = "dev_InventoryItem";
        private class InventoryItem
        {
            public int HashKey { get; set; }
            public int RangeKey { get; set; }
        }

        static void Main(string[] args)
        {
            Init();

            try
            {
                Console.WriteLine("Running test");
                Test();
                Console.WriteLine("Test succeeded");
            }
            catch (Exception e)
            {
                Console.WriteLine("Test failed");
                Console.WriteLine(e);
            }
            finally
            {
                Cleanup();
            }

            Console.WriteLine("Press enter to exit...");
            Console.ReadLine();
        }

        private static void Init()
        {
            using (var client = new AmazonDynamoDBClient(RegionEndpoint.USEast1))
            {
                bool tableExists = DoesTableExist(client);

                if (!tableExists)
                {
                    CreateTable(client);
                    WaitUntilTableActive(client);
                }
            }
        }
        private static void CreateTable(AmazonDynamoDBClient client)
        {
            var createRequest = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement { AttributeName = "HashKey", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "RangeKey", KeyType = KeyType.RANGE }
                },
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition { AttributeName = "HashKey", AttributeType = ScalarAttributeType.N },
                    new AttributeDefinition { AttributeName = "RangeKey", AttributeType = ScalarAttributeType.N }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1
                }
            };
            Console.WriteLine("Creating table");
            client.CreateTable(createRequest);
        }
        private static void WaitUntilTableActive(AmazonDynamoDBClient client)
        {
            Console.WriteLine("Waiting for table to be active");
            while (true)
            {
                var status = client.DescribeTable(tableName).Table.TableStatus;
                if (status == TableStatus.ACTIVE)
                    break;
                Console.WriteLine("Table status = {0}, sleeping...", status);
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            Console.WriteLine("Table created");
        }
        private static bool DoesTableExist(AmazonDynamoDBClient client)
        {
            bool tableExists;
            try
            {
                Console.WriteLine("Testing if table exists");
                client.DescribeTable(tableName);
                Console.WriteLine("Table exists");
                tableExists = true;
            }
            catch
            {
                Console.WriteLine("Table does not exist");
                tableExists = false;
            }

            return tableExists;
        }

        private static void Test()
        {
            var client = new AmazonDynamoDBClient(RegionEndpoint.USEast1);
            var context2 = new DynamoDBContext(client);

            var db = new DynamoDBOperationConfig();
            db.TableNamePrefix = "dev_";

            IEnumerable<InventoryItem> queryResults = context2.Query<InventoryItem>(clientIdentifier, db);
        }

        private static void Cleanup()
        {
            using (var client = new AmazonDynamoDBClient(RegionEndpoint.USEast1))
            {
                Console.WriteLine("Deleting table");
                client.DeleteTable(tableName);
            }
        }
    }
}
