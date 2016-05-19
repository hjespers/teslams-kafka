//Kafka Schema Registry commands
curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" --data ./streamdata.kafka.schema http://localhost:8081/subjects/teslams-value/versions