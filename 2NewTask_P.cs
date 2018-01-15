using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;

class NewTask
{
    public static void Main(string[] args)
    {
        var amqpList = new List<AmqpTcpEndpoint>
        {
            new AmqpTcpEndpoint(new Uri("amqp://cnsha063:5672")),
            new AmqpTcpEndpoint(new Uri("amqp://cnsha2179:5672"))
        };
        var factory = new ConnectionFactory()
        {
            UserName = "producer",
            Password = "producer",
            Port = 5672,
            VirtualHost = "davidvhost",
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };
        string exchangeName = "david.exchange";
        string queueName = "david.queue2";
        string routingKey = queueName;

        using (var connection = factory.CreateConnection(amqpList))
        {
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null); // durable = true

                var message = "Hello World! " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ffff");
                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true; // save the message to disk, not stronger guarantee

                channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: properties, body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
        }

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}