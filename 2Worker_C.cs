using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class Worker
{
    // open multi workers the same time to test
    public static void Main()
    {
        var amqpList = new List<AmqpTcpEndpoint>
        {
            new AmqpTcpEndpoint(new Uri("amqp://cnsha063:5672")),
            new AmqpTcpEndpoint(new Uri("amqp://cnsha2179:5672"))
        };
        var factory = new ConnectionFactory()
        {
            UserName = "consumer",
            Password = "consumer",
            Port = 5672,
            VirtualHost = "davidvhost",
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };
        string exchangeName = "david.exchange";
        string queueName = "david.queue2";
        string routingKey = queueName;

        using (var connection = factory.CreateConnection(amqpList))
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null); // durable = true
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false); // not to give more than one message to a worker at a time

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);

                Thread.Sleep(new Random().Next(1, 10) * 1000);

                Console.WriteLine(" [x] Done.");

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false); // ack after dealing here
            };
            channel.BasicConsume(queue: queueName, noAck: false, consumer: consumer); // no manual ack(nowledgement) set to false
            channel.QueueBind(queueName, exchangeName, routingKey, null);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}