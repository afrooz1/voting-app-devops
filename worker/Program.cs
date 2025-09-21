using System;
using System.Data.Common;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                // Connect to Postgres with retry
                var pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");

                // Connect to Redis with retry
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                // Keep-alive command for Postgres
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { vote = "", voter_id = "" };

                while (true)
                {
                    Thread.Sleep(100); // Slow down loop to reduce CPU usage

                    // Reconnect Redis if down
                    if (redisConn == null || !redisConn.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis...");
                        redisConn = OpenRedisConnection("redis");
                        redis = redisConn.GetDatabase();
                    }

                    string json = redis.ListLeftPop("votes");
                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");

                        // Reconnect DB if down
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB...");
                            pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                        }

                        UpdateVote(pgsql, vote.voter_id, vote.vote);
                    }
                    else
                    {
                        // Keep Postgres connection alive
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            while (true)
            {
                try
                {
                    var connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    Console.WriteLine("Connected to Postgres.");

                    // Ensure votes table exists
                    using var command = connection.CreateCommand();
                    command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                                id VARCHAR(255) NOT NULL UNIQUE,
                                                vote VARCHAR(255) NOT NULL
                                            )";
                    command.ExecuteNonQuery();

                    return connection;
                }
                catch (SocketException)
                {
                    Console.WriteLine("Waiting for Postgres...");
                    Thread.Sleep(1000);
                }
                catch (DbException)
                {
                    Console.WriteLine("Waiting for Postgres...");
                    Thread.Sleep(1000);
                }
            }
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            while (true)
            {
                try
                {
                    Console.WriteLine($"Connecting to Redis at {hostname}:6379");
                    var conn = ConnectionMultiplexer.Connect($"{hostname}:6379");
                    if (conn.IsConnected)
                    {
                        Console.WriteLine("Connected to Redis!");
                        return conn;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Waiting for Redis: {ex.Message}");
                    Thread.Sleep(1000);
                }
            }
        }

        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            using var command = connection.CreateCommand();
            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
        }
    }
}
