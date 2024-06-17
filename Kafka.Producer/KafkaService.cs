using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Kafka.Producer.Events;

namespace Kafka.Producer;

internal class KafkaService
{
    internal async Task CreateTopicAsync(string topicName)
    {
        // Topic oluşturma-silme gibi işlemlerde önce AdminClient'a bağlanmak gerekiyor.
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = "localhost:9094", // 1 den fazla broker ile çalışıyor olsaydık onların da adreslerini ekliyor olacaktık.
        }).Build();

        try
        {
            // Topic oluşturulurken default ayarların dışında configuration yapmak istersek, ilgili ayarları aşağıdaki gibi bir dictionary ile belirtebiliriz.
            var configs = new Dictionary<string, string>()
            {
                // Producer message'ı oluştururken timestamp değeri belirtilmemişse, default'da o anki zaman timestamp olarak atanır.
                // Fakat biz bu default ayarı message'ın üretildiği zaman yerine message'ın topic -> partition -> segment'e kaydedilip log basıldığı zaman olarak değiştiriyoruz.
                { "message.timestamp.type", "LogAppendTime" },

                // Retention Time = Saklama Süresi ms türündendir ve default olarak 7 gündür, değiştirmek istersek;
                // { "retention.ms", "-1" } // -1 ile Kafka'da message'lar herhangi bir süre kısıtı olmaksızın tutulur.
                { "retention.ms", TimeSpan.FromDays(30).TotalMilliseconds.ToString() }, // Kafka'da message'lar 30 gün boyunca tutulacaktır.

                // Retention Limit = Saklama Limiti byte türündendir ve default'u -1 ayarlıdır, yani herhangi bir limit söz konusu değildir.
                // { "retention.bytes", "100000000"} // Kafka'da partition içerisindeki message'ların boyutu 100 mb'a ulaştığında en eski message'lar silinerek yer açılacaktır.
            };

            await adminClient.CreateTopicsAsync(
            [
                // 1 broker üzerinde 3 partition dan oluşan "MyTopic" isimli bir topic oluşturuluyor. ReplicationFactor 1, çünkü tek bir broker'ımız var,
                // burada Leader'ın kendisi aynı zamanda replica olarak sayılıyor.
                new TopicSpecification() { Name = topicName, NumPartitions = 3, ReplicationFactor = 1, Configs = configs}
            ]);

            await Console.Out.WriteLineAsync($"{topicName} is successfully created.");
        }
        catch (Exception ex)
        {
            await Console.Out.WriteLineAsync(ex.Message);
        }
    }

    internal async Task SendComplexMessageAsync(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094",
            // Acks = Acks.All     // -1 Message'ı produce et ve kafkanın message'ı Leader Partition'a ve Replica'lara başarılı bir şekilde yazıp yazmadığı bilgisini dön.
            // Veri kayıplarının söz konusu dahi edilemeyeceği senaryolar için uygundur ve diğerlerine göre daha yavaştır.

            // Acks = Acks.None    // 0, Message'ı produce et ama kafkadan message'ın başarılı bir şekilde kayıt edildiğine dair herhangi bir bilgi bekleme.
            // Bazı veri kayıplarının tölere edilebileceği senaryolar için uygundur ve çok hızlıdır.

            Acks = Acks.Leader     // 1, Message'ı produce et ve kafkanın message'ı Leader Partition'a başarılı bir şekilde yazıp yazmadığı bilgisini dön.
                                   // Leader'dan replicalara message'ların kopyalanması sırasında Leader'ın crash olması durumunda veri kaybı yaşanabilir.

        };

        // Göndereceğimiz key, value çifti eğer primitive type ise ayrıca bir serialize işlemi yapmaya gerek yok.
        // Kafka implicit olarak serialize işlemini yapıyor. Eğer gönderdiğim key int yerine complex bir tip olsaydı
        // SetKeySerializer method'una CustomKeySerializer geçmem gerekicekti.
        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .Build();

        foreach (var index in Enumerable.Range(1, 100))
        {
            var header = new Headers
            {
                { "correlationId", "123456"u8.ToArray() },
                { "version", "v1"u8.ToArray() }
            };
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = index * 100,
                UserId = index
            };

            var message = new Message<int, OrderCreatedEvent>()
            {
                Key = index,
                Value = orderCreatedEvent,
                Headers = header
            };

            // Burada topic içerisindeki rastgele bir partition'a message üretiliyor.
            var result = await producer.ProduceAsync(topicName, message);

            // Burada ise topic içerisindeki belirlediğimiz bir partition'a message üretiliyor. Mesajlar, Index'i 2 olan yani 3.partition'a basılacaktır.
            // Buradaki amaç eğer Kafka Cluster'ımızda birden fazla broker var ise ve lokasyonları farklıysa bize en yakın partition üzerinde çalışmak daha performanslı olacaktır.
            // var result = await producer.ProduceAsync(new TopicPartition(topicName, 2), message);

            foreach (var pi in result.GetType().GetProperties())
            {
                await Console.Out.WriteLineAsync($"{pi.Name} ==> {pi.GetValue(result)}");
            }

            await Console.Out.WriteLineAsync("-------------------------------------------------");
        }

        await Console.Out.WriteLineAsync("All messages have been produced.\nPlease press any key if you want to produce more or press 'e' to exit.");
    }
}
