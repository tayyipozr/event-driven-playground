using Confluent.Kafka;
using Xunit.Abstractions;

namespace KafkaFundamentals.Tests;

public sealed class PartitioningTests(KafkaFixture kafka, ITestOutputHelper output)
    : IClassFixture<KafkaFixture>
{
    [Fact]
    public async Task Produce_ThenConsume_ReturnsSameMessage()
    {
        var topic = await kafka.CreateTopicAsync(partitions: 3);
        using var producer = CreateProducer();

        // The delivery report tells us exactly where the broker stored the message.
        var delivery = await producer.ProduceAsync(
            topic, new Message<string, string> { Key = "order-1", Value = "placed" });

        Assert.Equal(PersistenceStatus.Persisted, delivery.Status);

        using var consumer = CreateConsumer();

        // Read that exact partition and offset directly. No group join, no rebalance.
        consumer.Assign(delivery.TopicPartitionOffset);
        var record = consumer.Consume(TimeSpan.FromSeconds(10));

        Assert.NotNull(record);
        Assert.Equal("order-1", record.Message.Key);
        Assert.Equal("placed", record.Message.Value);
        Assert.Equal(delivery.Offset, record.Offset);
    }

    [Fact]
    public async Task SameKey_AlwaysLandsOnSamePartition_AndKeepsOrder()
    {
        var topic = await kafka.CreateTopicAsync(partitions: 3);
        using var producer = CreateProducer();

        // Other keys are interleaved, because in real life a partition is shared by many keys.
        (string Key, string Value)[] events =
        [
            ("order-1", "placed"),
            ("order-2", "placed"),
            ("order-1", "paid"),
            ("order-3", "placed"),
            ("order-1", "shipped")
        ];

        var order1Partitions = new List<int>();

        foreach (var (key, value) in events)
        {
            // Awaiting each send keeps the send order deterministic.
            var delivery = await producer.ProduceAsync(
                topic, new Message<string, string> { Key = key, Value = value });

            if (key == "order-1")
                order1Partitions.Add(delivery.Partition.Value);
        }

        // A brand-new producer picks the same partition. The client hashes the key;
        // the broker does not remember where a key went before.
        using (var secondProducer = CreateProducer())
        {
            var delivery = await secondProducer.ProduceAsync(
                topic, new Message<string, string> { Key = "order-1", Value = "delivered" });

            order1Partitions.Add(delivery.Partition.Value);
        }

        var partition = Assert.Single(order1Partitions.Distinct());

        var records = ConsumeWholePartition(new TopicPartition(topic, new Partition(partition)));
        var order1Values = records
            .Where(r => r.Message.Key == "order-1")
            .Select(r => r.Message.Value);

        Assert.Equal(new[] { "placed", "paid", "shipped", "delivered" }, order1Values);
    }

    [Fact]
    public async Task DifferentKeys_SpreadAcrossPartitions()
    {
        var topic = await kafka.CreateTopicAsync(partitions: 3);
        using var producer = CreateProducer();

        var partitionByKey = new Dictionary<string, int>();

        for (var i = 1; i <= 20; i++)
        {
            var key = $"order-{i}";
            var delivery = await producer.ProduceAsync(
                topic, new Message<string, string> { Key = key, Value = "placed" });

            partitionByKey[key] = delivery.Partition.Value;
        }

        foreach (var group in partitionByKey.GroupBy(kv => kv.Value).OrderBy(g => g.Key))
        {
            output.WriteLine(
                $"P{group.Key}: {group.Count()} keys ({string.Join(", ", group.Select(kv => kv.Key))})");
        }

        // 20 keys on 3 partitions: partitions are shared, but the hash uses more than one.
        Assert.True(partitionByKey.Values.Distinct().Count() > 1);
    }

    private IProducer<string, string> CreateProducer() =>
        new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = kafka.BootstrapServers }).Build();

    private IConsumer<string, string> CreateConsumer() =>
        new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = "test-reader",  // required by the client, unused with Assign
            EnableAutoCommit = false
        }).Build();

    // Reads one partition from offset 0 up to its current end.
    // Assumes a fresh topic, so the first offset is 0.
    private List<ConsumeResult<string, string>> ConsumeWholePartition(TopicPartition tp)
    {
        using var consumer = CreateConsumer();

        // High watermark = LOG-END-OFFSET in the CLI: the offset the next message will get.
        var end = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).High;

        consumer.Assign(new TopicPartitionOffset(tp, Offset.Beginning));

        var records = new List<ConsumeResult<string, string>>();
        while (records.Count < end.Value)
        {
            var record = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(record); // fail fast instead of hanging if a message is missing
            records.Add(record);
        }

        return records;
    }
}