using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class ReceiveLogsTopic
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
            UserName = "consumer",
            Password = "consumer",
            Port = 5672,
            VirtualHost = "davidvhost",
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using (var connection = factory.CreateConnection(amqpList))
        using (var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
            var queueName = channel.QueueDeclare().QueueName;

            Console.WriteLine("  * (star) can substitute for exactly one word.");
            Console.WriteLine("  # (hash) can substitute for zero or more words.");
            Console.WriteLine("  Example: \"anonymous.info\", \" *.orange.* \", \" *.*.rabbit\" and \"lazy.#\"");
            Console.WriteLine("Input for identify multi consumer as routingKey, split with \",\":");
            var inputRoutingKey = Console.ReadLine().Trim();
            inputRoutingKey = (!string.IsNullOrWhiteSpace(inputRoutingKey)) ? inputRoutingKey : "anonymous.info";
            foreach (var bindingRoutingKey in inputRoutingKey.Split(','))
            {
                channel.QueueBind(queue: queueName, exchange: "topic_logs", routingKey: bindingRoutingKey);
            }

            Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

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
        }
    }
}