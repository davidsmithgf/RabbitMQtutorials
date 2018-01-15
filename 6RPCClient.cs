using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class RPC
{
    public static void Main()
    {
        var rpcClient = new RPCClient();

        var message = "Hello World! " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ffff");
        Console.WriteLine(message);

        Console.WriteLine(rpcClient.Call(message));

        rpcClient.Close();

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}

class RPCClient
{
    private IConnection connection;
    private IModel channel;
    private string exchangeName;
    private string routingKey;
    private string replyQueueName;
    private EventingBasicConsumer consumer;

    public RPCClient()
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
        exchangeName = "RPC";
        routingKey = "rpc_queue";

        connection = factory.CreateConnection(amqpList);
        channel = connection.CreateModel();
        replyQueueName = channel.QueueDeclare().QueueName; // anonymous queue for reply
        consumer = new EventingBasicConsumer(channel);
        channel.BasicConsume(queue: replyQueueName, noAck: true, consumer: consumer);
        channel.QueueBind(replyQueueName, exchangeName, replyQueueName, null); // bind to anonymous queue
    }

    public string Call(string message)
    {
        var corrId = Guid.NewGuid().ToString();
        var props = channel.CreateBasicProperties();
        props.ReplyTo = replyQueueName;
        props.CorrelationId = corrId;

        channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: props, body: Encoding.UTF8.GetBytes(message));
        Console.WriteLine(" [x] Sent {0}", message);

        string response = string.Empty;
        try
        {
            consumer.Received += (model, ea) =>
            {
                if (ea.BasicProperties.CorrelationId == corrId)
                {
                    response = " [.] <= {" + ea.RoutingKey + "} " + Encoding.UTF8.GetString(ea.Body);
                }
            };
            while (response == string.Empty) ; // Wait for reply.
        }
        catch (Exception ex)
        {
            response = ex.ToString();
        }
        return response;
    }

    public void Close()
    {
        connection.Close();
    }
}