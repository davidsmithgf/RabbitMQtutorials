using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;

class EmitLog
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

        using (var connection = factory.CreateConnection(amqpList))
        {
            using (var channel = connection.CreateModel())
            {
                //The exchange must know exactly what to do with a message it receives.
                //Should it be appended to a particular queue?
                //Should it be appended to many queues?
                //Or should it get discarded.
                //The rules for that are defined by the exchange type.
                //There are a few exchange types available: direct, topic, headers and fanout.
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                var message = "Hello World! " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ffff");
                var body = Encoding.UTF8.GetBytes(message);

                // routingKey
                // routed to the queue with the name specified by routingKey, if it exists.
                // its value is ignored for fanout exchanges
                channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
        }

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}