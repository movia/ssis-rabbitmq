using Microsoft.SqlServer.Dts.Runtime;

using RabbitMQ.Client;

namespace RabbitMQConnectionManager
{
    [DtsConnection(       
        IconResource = "RabbitMQConnectionManager.Rabbit.ico",
        ConnectionType = "RABBITMQ",
        DisplayName = "RabbitMQ Connection Manager",
        Description = "Connection Manager for RabbitMQ",
        UITypeName = "RabbitMQConnectionManager.RabbitMQConnectionManagerUI, RabbitMQConnectionManager, Version=13.0.1.0, Culture=neutral, PublicKeyToken=ac1c316408dd3955")]
    public class RabbitMQConnectionManager : ConnectionManagerBase
    {
        public string HostName { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public int Port { get; set; }
        public string VirtualHost { get; set; }

        public RabbitMQConnectionManager()
        {
            HostName = "localhost";
            UserName = "guest";
            Password = "guest";
            Port = 5672;
            VirtualHost = "/";
        }

        public override DTSExecResult Validate(IDTSInfoEvents infoEvents)
        {
            if (string.IsNullOrEmpty(HostName))
            {
                return DTSExecResult.Failure;
            }
            else if (string.IsNullOrEmpty(VirtualHost))
            {
                return DTSExecResult.Failure;
            }
            else if (string.IsNullOrEmpty(UserName))
            {
                return DTSExecResult.Failure;
            }
            else if (string.IsNullOrEmpty(Password))
            {
                return DTSExecResult.Failure;
            }
            else if (Port <= 0)
            {
                return DTSExecResult.Failure;
            }

            return DTSExecResult.Success;
        }

        public override object AcquireConnection(object txn)
        {
            ConnectionFactory connFactory = new ConnectionFactory()
            {
                UserName = UserName,
                HostName = HostName,
                Password = Password,
                Port = Port,
                VirtualHost = VirtualHost
            };

            var connection = connFactory.CreateConnection();

            return connection;
        }

        public override void ReleaseConnection(object connection)
        {
            if (connection != null)
            {
                var conn = connection as IConnection;

                if (conn != null && conn.IsOpen)
                {
                    conn.Close();
                }
            }
        }
    }
}
