gacutil /u "RabbitMQ.Client" /f /nologo
gacutil /u "RabbitMQConnectionManager" /f /nologo
gacutil /u "RabbitMQSource" /f /nologo

gacutil /i "RabbitMQ.Client.dll" /f /nologo
gacutil /i "RabbitMQConnectionManager.dll" /f /nologo
gacutil /i "RabbitMQSource.dll" /f /nologo

gacutil /l "RabbitMQ.Client" /nologo
gacutil /l "RabbitMQConnectionManager" /nologo
gacutil /l "RabbitMQSource" /nologo

copy "RabbitMQConnectionManager.dll" "C:\Program Files\Microsoft SQL Server\130\DTS\Connections"
copy "RabbitMQConnectionManager.dll" "C:\Program Files (x86)\Microsoft SQL Server\130\DTS\Connections"

copy "RabbitMQSource.dll" "C:\Program Files\Microsoft SQL Server\130\DTS\PipelineComponents"
copy "RabbitMQSource.dll" "C:\Program Files (x86)\Microsoft SQL Server\130\DTS\PipelineComponents"

copy "RabbitMQExtensions.xml" "C:\Program Files\Microsoft SQL Server\130\DTS\UpgradeMappings"
copy "RabbitMQExtensions.xml" "C:\Program Files (x86)\Microsoft SQL Server\130\DTS\UpgradeMappings"

pause