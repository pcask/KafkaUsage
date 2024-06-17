// See https://aka.ms/new-console-template for more information
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer\n");

string topicName = "myTopic";

var kafkaService = new KafkaService();

await kafkaService.ConsumeComplexMessageAsync(topicName);

Console.ReadLine();