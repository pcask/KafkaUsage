using Confluent.Kafka;
using Kafka.Consumer.Events;
using System.Text;

namespace Kafka.Consumer;

internal class KafkaService
{
    internal async Task ConsumeComplexMessageAsync(string topicName)
    {
        try
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                GroupId = "group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false // Arka planda otomatik olarak gerçekleşen commit işlemini iptal eder.
                                         // Bu sayede biz message'ı başarılı bir şekilde işlediğimizde, manuel olarak commit gerçekleştirebiliriz.
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .Build();

            // Burada topic içerisindeki herhangi bir partition'dan okumanın yapılması sağlanıyor.
            consumer.Subscribe(topicName);

            // Burada ise sadece belirttiğimiz (2. partition) partition'lardan okumanın yapılması sağlanıyor.
            // consumer.Assign(new TopicPartition(topicName, 2));

            // Burada ise 2.Partition, 71.Offset'den sonra okumaya başlayacaktır.
            // Yani mesajlar daha önceden okunmuş olsada offset değerini 71 e geri çektiği için 71'den başlayıp tekrar son offset'e kadar okuma yapacaktır.
            // consumer.Assign(new TopicPartitionOffset(topicName, 2, 71));

            while (true)
            {
                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    try
                    {
                        var orderCreatedEvent = consumeResult.Message.Value;
                        var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlationId"));
                        var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));

                        await Console.Out.WriteLineAsync($"Headers\n" +
                            $"CurrentOffset = {consumeResult.Offset} \n" +
                            $"CorrelationId = {correlationId} \n" +
                            $"Version = {version} \n" +
                            $"Received message \n" +
                            $"Key = {consumeResult.Message.Key} \n" +
                            $"OrderCode = {orderCreatedEvent.OrderCode} \n" +
                            $"UserId = {orderCreatedEvent.UserId} \n" +
                            $"TotalPrice = {orderCreatedEvent.TotalPrice}");

                        // İlgili message'ı başarılı bir şekilde işlediysek, ilgili message'ı commit ederek offset değerini manuel olarak ilerletiyoruz.
                        consumer.Commit(consumeResult);
                    }
                    catch (Exception ex)
                    {
                        await Console.Out.WriteLineAsync(ex.Message);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            await Console.Out.WriteLineAsync(ex.Message);
        }
    }
}
