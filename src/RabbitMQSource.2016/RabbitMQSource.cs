using System;
using System.Text;

using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using RabbitMQSource.Enums;

namespace RabbitMQSource
{
    [DtsPipelineComponent(
        IconResource = "RabbitMQSource.Rabbit.ico",
        DisplayName = "RabbitMQ Source",
        ComponentType = ComponentType.SourceAdapter,
        Description = "Connection source for RabbitMQ",
        CurrentVersion = 2,
        UITypeName = "RabbitMQSource.RabbitMQSourceUI, RabbitMQSource, Version=13.0.1.0, Culture=neutral, PublicKeyToken=ac1c316408dd3955")]
    public class RabbitMQSource : PipelineComponent
    {
        private const string defaultBodyColumnName = "Body";
        private const string defaultRoutingKeyColumnName = "RoutingKey";
        private const string defaultTimestampColumnName = "Timestamp";

        private IConnection rabbitConnection;
        private IModel consumerChannel;

        private RabbitMQConnectionManager.RabbitMQConnectionManager rabbitMqConnectionManager;

        private string queueName;
        private bool declareQueue;
        private int timeout;
        private int batchSize;
        private string consumerTag;

        public int[] outputToBufferMap;

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp);
            return dtDateTime;
        }

        public override void ProvideComponentProperties()
        {
            // Reset the component.
            RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            IDTSCustomProperty100 queueName = ComponentMetaData.CustomPropertyCollection.New();
            queueName.Name = "QueueName";
            queueName.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            queueName.Description = "The name of the RabbitMQ queue to read messages from";

            IDTSCustomProperty100 declareQueue = ComponentMetaData.CustomPropertyCollection.New();
            declareQueue.Name = "DeclareQueue";
            declareQueue.Description = "If true, the source will declare the queue.";
            declareQueue.TypeConverter = typeof(TrueFalseProperty).AssemblyQualifiedName;
            declareQueue.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            declareQueue.Value = TrueFalseProperty.False;

            IDTSCustomProperty100 timeoutProperty = ComponentMetaData.CustomPropertyCollection.New();
            timeoutProperty.Name = "Timeout";
            timeoutProperty.Description = "Max. time in seconds to pull messages.";
            timeoutProperty.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            timeoutProperty.Value = 1;

            IDTSCustomProperty100 batchSizeProperty = ComponentMetaData.CustomPropertyCollection.New();
            batchSizeProperty.Name = "BatchSize";
            batchSizeProperty.Description = "Max. batch size to consume.";
            batchSizeProperty.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            batchSizeProperty.Value = 10000;

            IDTSRuntimeConnection100 connection = ComponentMetaData.RuntimeConnectionCollection.New();
            connection.Name = "RabbitMQ";
            connection.ConnectionManagerID = "RabbitMQ";

            CreateColumns();
        }

        public override void ReinitializeMetaData()
        {
            base.ReinitializeMetaData();

            CreateColumns();
        }

        private void CreateColumns()
        {
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];

            output.OutputColumnCollection.RemoveAll();
            output.ExternalMetadataColumnCollection.RemoveAll();

            IDTSOutputColumn100 column1 = output.OutputColumnCollection.New();
            column1.Name = defaultBodyColumnName;
            column1.SetDataTypeProperties(DataType.DT_IMAGE, 0, 0, 0, 0);
            CreateExternalMetaDataColumn(output, column1);

            IDTSOutputColumn100 column2 = output.OutputColumnCollection.New();
            column2.Name = defaultRoutingKeyColumnName;
            column2.SetDataTypeProperties(DataType.DT_WSTR, 100, 0, 0, 0);
            CreateExternalMetaDataColumn(output, column2);

            IDTSOutputColumn100 column3 = output.OutputColumnCollection.New();        
            column3.Name = defaultTimestampColumnName;
            column3.SetDataTypeProperties(DataType.DT_DATE, 0, 0, 0, 0);
            CreateExternalMetaDataColumn(output, column3);
        }

        private void CreateExternalMetaDataColumn(IDTSOutput100 output, IDTSOutputColumn100 outputColumn)
        {
            // Set the properties of the external metadata column.
            IDTSExternalMetadataColumn100 externalColumn = output.ExternalMetadataColumnCollection.New();
            externalColumn.Name = outputColumn.Name;
            externalColumn.Precision = outputColumn.Precision;
            externalColumn.Length = outputColumn.Length;
            externalColumn.DataType = outputColumn.DataType;
            externalColumn.Scale = outputColumn.Scale;

            // Map the external column to the output column.
            outputColumn.ExternalMetadataColumnID = externalColumn.ID;
        }

        public override IDTSOutput100 InsertOutput(DTSInsertPlacement insertPlacement, int outputID)
        {
            throw new InvalidOperationException();
        }

        public override IDTSOutputColumn100 InsertOutputColumnAt(int outputId, int outputColumnIndex, string name, string description)
        {
            var output = ComponentMetaData.OutputCollection.FindObjectByID(outputId);

            var column = base.InsertOutputColumnAt(outputId, outputColumnIndex, name, description);
            //CreateExternalMetaDataColumn(output, column);        

            return column;
        }

        public override void SetOutputColumnDataTypeProperties(int outputId, int outputColumnId, DataType dataType, int length, int precision, int scale, int codePage)
        {
            var output = ComponentMetaData.OutputCollection.FindObjectByID(outputId);
            var columnIndex = output.OutputColumnCollection.FindObjectIndexByID(outputColumnId);
            var column = output.OutputColumnCollection.FindObjectByID(outputColumnId);

            /* We do not want users to change the data type of body, routing key and timestamp. */
            if (columnIndex <= 2)
                throw new InvalidOperationException($"Cannot change datatype of '{column.Name}'.");

            column.SetDataTypeProperties(dataType, length, precision, scale, codePage);
        }

        public override DTSValidationStatus Validate()
        {
            bool cancel;
            string qName = (string)ComponentMetaData.CustomPropertyCollection["QueueName"].Value;            

            if (string.IsNullOrWhiteSpace(qName))
            {
                //Validate that the QueueName property is set
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "The QueueName property must be set", "", 0, out cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return base.Validate();
        }

        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager != null)
            {
                ConnectionManager connectionManager = DtsConvert.GetWrapper(ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager);

                this.rabbitMqConnectionManager = connectionManager.InnerObject as RabbitMQConnectionManager.RabbitMQConnectionManager;

                if (rabbitMqConnectionManager == null)
                    throw new Exception("Couldn't get the RabbitMQ connection manager, ");

                queueName = (string)ComponentMetaData.CustomPropertyCollection["QueueName"].Value;
                timeout = (int)ComponentMetaData.CustomPropertyCollection["Timeout"].Value;
                batchSize = (int)ComponentMetaData.CustomPropertyCollection["BatchSize"].Value;

                rabbitConnection = rabbitMqConnectionManager.AcquireConnection(transaction) as IConnection;
            }
        }

        public override void ReleaseConnections()
        {
            if (rabbitMqConnectionManager != null)
            {
                this.rabbitMqConnectionManager.ReleaseConnection(rabbitConnection);
            }
        }

        public override void PreExecute()
        {
            var output = ComponentMetaData.OutputCollection[0];

            /* Parse properties */
            declareQueue = (TrueFalseProperty)ComponentMetaData.CustomPropertyCollection["DeclareQueue"].Value == TrueFalseProperty.True;
            
            /* Initialize Output to Buffer Map */            
            outputToBufferMap = new int[output.OutputColumnCollection.Count];

            for (int i = 0; i < output.OutputColumnCollection.Count; i++)
            {
                /* Here, "i" is the column count in the component's outputcolumncollection
                   and the value of mapOutputColsToBufferCols[i] is the index of the corresponding column in the
                   buffer. */
                outputToBufferMap[i] = BufferManager.FindColumnByLineageID(output.Buffer, output.OutputColumnCollection[i].LineageID);
            }

            try
            {
                consumerChannel = rabbitConnection.CreateModel();
                if (declareQueue)
                    consumerChannel.QueueDeclare(queueName, true, false, false, null);
            }
            catch (Exception)
            {
                ReleaseConnections();
                throw;
            }
        }

        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            DateTime startTime = DateTime.UtcNow;
            int numRows = 0;
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];
            PipelineBuffer buffer = buffers[0];

            BasicGetResult result;

            while (numRows < batchSize && (DateTime.UtcNow - startTime).TotalSeconds < timeout)
            {
                try
                {
                    result = consumerChannel.BasicGet(queueName, true);
                }
                catch (Exception)
                {
                    break;
                }

                if (result != null)
                {
                    numRows++;
                    buffer.AddRow();

                    for (int i = 0; i < output.OutputColumnCollection.Count; i++)
                    {
                        /* Handles body, routing key and timestamp: */
                        if (string.Equals(output.OutputColumnCollection[i].Name, defaultBodyColumnName))
                            buffer[outputToBufferMap[i]] = result.Body;
                        else if (string.Equals(output.OutputColumnCollection[i].Name, defaultRoutingKeyColumnName))
                            buffer[outputToBufferMap[i]] = result.RoutingKey;
                        else if (string.Equals(output.OutputColumnCollection[i].Name, defaultTimestampColumnName))
                            buffer[outputToBufferMap[i]] = UnixTimeStampToDateTime(result.BasicProperties.Timestamp.UnixTime);
                        /* For all other columns see if a header matched the name: */
                        else if (result.BasicProperties.Headers.ContainsKey(output.OutputColumnCollection[i].Name.ToLower()))
                        {
                            var column = output.OutputColumnCollection[i];
                            var data = result.BasicProperties.Headers[output.OutputColumnCollection[i].Name.ToLower()];

                            if (column.DataType == DataType.DT_WSTR && data is byte[])
                                buffer[outputToBufferMap[i]] = Encoding.UTF8.GetString((byte[])data);
                            else
                                buffer[outputToBufferMap[i]] = result.BasicProperties.Headers[output.OutputColumnCollection[i].Name.ToLower()];
                        }
                        /* Otherwise, simply set output null */
                        else
                        {
                            buffer.SetNull(outputToBufferMap[i]);
                        }
                    }
                    
                }
                else
                {
                    System.Threading.Tasks.Task.Delay(500).Wait();
                }
            }

            buffer.SetEndOfRowset();
        }

        public override void Cleanup()
        {
            if (consumerChannel.IsOpen)
            {
                consumerChannel.Close();
            }
            base.Cleanup();
        }

        public override void PerformUpgrade(int pipelineVersion)
        {
            DtsPipelineComponentAttribute attribute = (DtsPipelineComponentAttribute)Attribute.GetCustomAttribute(this.GetType(), typeof(DtsPipelineComponentAttribute), false);
            int currentVersion = attribute.CurrentVersion;
            ComponentMetaData.Version = currentVersion;
        }
    }
}
