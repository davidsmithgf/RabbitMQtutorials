using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;

class EmitLogTopic
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
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");

                Console.WriteLine("  * (star) can substitute for exactly one word.");
                Console.WriteLine("  # (hash) can substitute for zero or more words.");
                Console.WriteLine("  Example: \"anonymous.info\", \" *.orange.* \", \" *.*.rabbit\" and \"lazy.#\"");
                Console.WriteLine("Input for identify multi producer as routingKey:");
                var routingKey = Console.ReadLine().Trim();
                routingKey = (!string.IsNullOrWhiteSpace(routingKey)) ? routingKey : "anonymous.info";

                var message = "Hello World! " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ffff");
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "topic_logs", routingKey: routingKey, basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
            }
        }
    }
}