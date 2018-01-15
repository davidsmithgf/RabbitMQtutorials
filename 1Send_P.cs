using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;

class Send
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
            UserName = "producer",
            Password = "producer",
            Port = 5672,
            VirtualHost = "davidvhost",
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
            //Uri = "amqp://producer:producer@cnsha063:5672/davidvhost"
        };
        string exchangeName = "david.exchange";
        string queueName = "david.queue1";
        string routingKey = queueName;

        using (var connection = factory.CreateConnection(amqpList))
        {
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                string message = "Hello World! " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ffff");
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
        }

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}