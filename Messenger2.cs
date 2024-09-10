using Confluent.Kafka;

internal class Messenger2
{

    private const string BootstrapServers = "localhost:9092"; // Replace with your Kafka broker address
    private const string FirstMess = "producer-topic";
    private const string SecondMess = "consumer-topic";
    private const string ConsumerGroupId = "my-consumer-group";

    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        var consumerTask = Task.Run(() => RunConsumer(cts.Token), cts.Token);
        var producerTask = Task.Run(() => RunProducer(cts.Token), cts.Token);

        Task.WaitAll(consumerTask, producerTask);
    }

    private static void RunProducer(CancellationToken cancellationToken)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                while (true)
                {

                    Console.WriteLine("Enter message to produce (or 'exit' to stop):");
                    var value = Console.ReadLine();

                    if (value?.ToLower() == "exit")
                    {
                        break;
                    }

                    producer.Produce(SecondMess, new Message<Null, string> { Value = value });
                }
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }

    private static void RunConsumer(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            GroupId = ConsumerGroupId,
            BootstrapServers = BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Latest,

        };

        using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
        {
            consumer.Subscribe(FirstMess);

            try
            {
                while (true)
                {

                    var consumeResult = consumer.Consume(cancellationToken);
                    Console.WriteLine($"Consumed: {consumeResult.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation
            }
            finally
            {
                consumer.Close();
            }
        }
    }

}