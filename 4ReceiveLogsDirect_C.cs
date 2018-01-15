using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class ReceiveLogsDirect
{
    public static int Main(string[] args)
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

        using (var connection = factory.CreateConnection(amqpList))
        {
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");
                var queueName = channel.QueueDeclare().QueueName; // anonymous queue

                Console.WriteLine("Input for identify multi consumer as routingKey, split with \",\" - info,a,b,c,d ...");
                var severity = Console.ReadLine().Trim();
                severity = (!string.IsNullOrWhiteSpace(severity)) ? severity : "info";
                foreach (var s in severity.Split(','))
                {
                    if (!string.IsNullOrWhiteSpace(s))
                        channel.QueueBind(queue: queueName, exchange: "direct_logs", routingKey: s); // bind multi routingKey
                }

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
                };
                channel.BasicConsume(queue: queueName, noAck: true, consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
                return 0;
            }
        }
    }
}