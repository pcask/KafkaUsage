// See https://aka.ms/new-console-template for more information

using Kafka.Producer;

Console.WriteLine("Kafka Producer\n");

string topicName = "myTopic";

var kafkaService = new KafkaService();

await kafkaService.CreateTopicAsync(topicName);

do
{
    await kafkaService.SendComplexMessageAsync(topicName);
}
while (Console.ReadLine() != "e");
