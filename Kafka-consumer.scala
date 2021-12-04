// Databricks notebook source
// Import login module
import kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
val TOPIC = "demo-test"

// Update values as needed
val BOOTSTRAP_SERVERS = "demo-kafka.servicebus.windows.net:9093"

// NEW VALUE, REPLACE EVENTHUBSNAMESPACE, SECRETKEYNAME, SECRETKEYVALUE WITH YOUR OWN VALUES
val EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://demo-kafka.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1+FK3hBa+/DQqE/Ua2kefUsBX1U9KqbN1ZIYbSF6vbI=\";"

// READ STREAM USING SPARK's KAFKA CONNECTOR
val rates = spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .option("kafka.group.id", "$Default")
    .load()


// COMMAND ----------

rates
    .selectExpr("CAST(key as STRING)", "CAST(value as STRING)")
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", false).start().awaitTermination()

// COMMAND ----------


