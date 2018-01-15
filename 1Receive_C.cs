using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class Receive
{
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
            //Uri = "amqp://consumer:consumer@cnsha063:5672/davidvhost"
        };
        string exchangeName = "david.exchange";
        string queueName = "david.queue1";
        string routingKey = queueName;

        using (var connection = factory.CreateConnection(amqpList))
        {
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };
                channel.BasicConsume(queue: queueName, noAck: true, consumer: consumer);
                channel.QueueBind(queueName, exchangeName, routingKey, null);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}