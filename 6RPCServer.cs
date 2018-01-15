using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class RPCServer
{
    // open multi server the same time to test dealing client message
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
        string exchangeName = "RPC";
        string queueName = "rpc_queue";
        string routingKey = queueName;

        using (var connection = factory.CreateConnection(amqpList))
        {
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: exchangeName, type: "direct");

                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.BasicQos(0, 1, false);

                Console.WriteLine(" [x] Awaiting RPC requests...");

                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queue: queueName, noAck: false, consumer: consumer);
                channel.QueueBind(queueName, exchangeName, queueName, null);
                consumer.Received += (model, ea) =>
                {
                    string response = null;

                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = ea.BasicProperties.CorrelationId; // CorrelationId: to correlate RPC responses with requests
                    // DeliveryMode: marks a message as persistent or transient
                    // ContentType: describe the mime-type, application/json for JSON
                    var replyTo = ea.BasicProperties.ReplyTo; // ReplyTo: name a callback queue, routingKey

                    try
                    {
                        var message = Encoding.UTF8.GetString(ea.Body);
                        response = "Reply " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ffff") + " for " + message;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(" [.] " + e.Message);
                        response = "";
                    }
                    finally
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        Console.WriteLine(" [.] => {" + replyTo + "} " + response);
                        channel.BasicPublish(exchange: exchangeName, routingKey: replyTo, basicProperties: replyProps, body: responseBytes);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                };

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}