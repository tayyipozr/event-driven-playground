using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;

namespace KafkaFundamentals.Tests;

// One single-node KRaft broker shared by every test in a class.
// xUnit creates a new test class instance per test, so the container lives here instead.
public sealed class KafkaFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("apache/kafka:4.3.1")
        .WithEnvironment("KAFKA_HEAP_OPTS", "-Xms512m -Xmx512m")
        .Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    // Each test gets its own topic so tests never see each other's messages.
    public async Task<string> CreateTopicAsync(int partitions)
    {
        var topic = $"orders-{Guid.NewGuid():N}";

        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = partitions,
                ReplicationFactor = 1 // single broker in tests
            }
        ]);

        return topic;
    }
}