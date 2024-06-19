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
            // 1 tane broker'ın adresini vermemiz yeterli olur. Kafka cluster içerisindeki ayakta olan brokerları kendi bulacaktır.
            // Fakat adresini verdiğimiz broker'ın ayakta olmama ihtimali var bu nedenle
            // 1 den fazla broker ile çalıştığımızda hepsinin adresini belirtmek best-practice olacaktır.
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
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

                // Acknowledgement değeri All (-1) atandığında, min.insync.replicas değeri kadar replica'ya veri yazıldığını garanti eder.
                // Örneğin ReplicationFactor 6 ama min.insync.replicas değeri 3 ise, bir partition için 3 tane replica'ya veri yazılır yazılmaz acks bilgisi başarılı döner.
                { "min.insync.replicas", "3" }
            };

            await adminClient.CreateTopicsAsync(
            [
                // 3 broker üzerinde 6 partition'dan oluşan bir topic oluşturuluyor. 3 broker'ımız olduğu için ReplicationFactor en fazla 3 olabilir,
                // burada Leader'ın kendisi aynı zamanda 1 replica olarak sayılıyor.
                new TopicSpecification() { Name = topicName, NumPartitions = 6, ReplicationFactor = 3, Configs = configs}
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
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
            Acks = Acks.All,     // -1 Message'ı produce et ve kafkanın message'ı Leader Partition'a ve Replica'lara başarılı bir şekilde yazıp yazmadığı bilgisini dön.
                                 // Veri kayıplarının söz konusu dahi edilemeyeceği senaryolar için uygundur ve diğerlerine göre daha yavaştır.

            // Acks = Acks.None    // 0, Message'ı produce et ama kafkadan message'ın başarılı bir şekilde kayıt edildiğine dair herhangi bir bilgi bekleme.
            // Bazı veri kayıplarının tölere edilebileceği senaryolar için uygundur ve çok hızlıdır.

            // Acks = Acks.Leader,    // 1, Message'ı produce et ve kafkanın message'ı Leader Partition'a başarılı bir şekilde yazıp yazmadığı bilgisini dön.
            // Leader'dan replicalara message'ların kopyalanması sırasında Leader'ın crash olması durumunda veri kaybı yaşanabilir.

            // Default retry sayısı sonsuzdur.
            // Acknowledgement All (-1) ile configure edilmişken, Producer "min.insync.replicas" değerini karşılayacak kadar replica'ya message yazmayı dener.
            // Bu denemelerin max. sayısı "MessageSendMaxRetries" ile berlirtilir, ve max değeri aşıldığında hata fırlatır.
            // Örneğin 3 broker'lı bir cluster'da "ReplicationFactor" 3, "Acks = Acks.All" ve "min.insync.replicas" 3 olarak configure edildiğini varsayalım,
            // Bu senaryoda eğer bir broker'a ulaşılamazsa, "min.insync.replicas" değeri karşılanamayacağı için retry mekanızması devreye girecektir
            // ve "MessageSendMaxRetries" değeri kadar retry'ın ardından broker'a hâla ulaşılamazsa hata fırlatacaktır. 
            // MessageSendMaxRetries = 10,

            // "MessageSendMaxRetries" parametresi için geçerli olanlar "MessageTimeoutMs" parametresi içinde geçerlidir.
            // Kafka yukarıdaki gibi belirli bir deneme sayısından ziyade Retry için Timeout süresinin belirlenmesini tavsiye ediyor.
            // MessageTimeoutMs = 5000,

            RetryBackoffMaxMs = 2000,   // Max. retry bekleme süresi. Default'u 100 ms dir.
            RetryBackoffMs = 100,       // İlk başarısız denemenin ardından 100 ms bekler ve "RetryBackoffMaxMs" değerini geçene kadar her denemede üstsel olarak artacaktır.
                                        // 1. denemede  => 100
                                        // 2. denemede  => 200
                                        // 3. denemede  => 400
                                        // 4. denemede  => 800
                                        // 5. denemede  => 1600
                                        // 6. denemede  => 1600 // 2000'i geçemeyeceği için "MessageSendMaxRetries" değerine ulaşana kadar 1600 ms de sabit kalır.

            MessageSendMaxRetries = 10  // ve max. 10 kez dene. Yani 18. saniyede "min.insync.replicas" değeri karşılanamazsa hata fırlatacaktır.
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

            try
            {
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
            catch (Exception ex)
            {
                await Console.Out.WriteLineAsync(ex.Message);
            }

        }

        await Console.Out.WriteLineAsync("All messages have been produced.\nPlease press any key if you want to produce more or press 'e' to exit.");
    }
}
