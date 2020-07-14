package com.app;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.Properties;

import static com.mongodb.client.model.Filters.eq;

public class ReadKafkaWriteMongoMain {

    private static MongoCollection<Document> mongoCollection;


    public static void main(String[] args) throws Exception{

        connectToMongo();
        consumeOutput("localhost:9092");

    }

    private static void connectToMongo(){
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        MongoDatabase database = mongoClient.getDatabase("Kafka_Mongo");
        mongoCollection = database.getCollection("test", Document.class)
                .withReadPreference(ReadPreference.primaryPreferred());
    }


    private static void upsert(final ConsumerRecord<String, String> consumerRecord) {
        String key = consumerRecord.key();
        Document value = Document.parse(consumerRecord.value());
        long numberOfDocuments = mongoCollection.count(getMatchQuery(key));
        if (numberOfDocuments == 1) {
            UpdateResult result = mongoCollection.replaceOne(getMatchQuery(key), value);
            if (!result.wasAcknowledged()  || result.getModifiedCount() != 1) {
                // Log and throw an error
                System.out.println("Acknowledgement received: " + result.wasAcknowledged() +
                        ".  Modified count: " + result.getModifiedCount() + " for record: " + key);
            }
        } else {
            mongoCollection.insertOne(value);
        }
        System.out.println("Written to mongo with key:" + consumerRecord.key());
    }

    private static Bson getMatchQuery(String key) {
        return eq("_id", key);
    }


    private static void consumeOutput(String bootstrapServers) throws Exception{
        System.out.println("Consumer called");
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "test-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties,
                Serdes.String().deserializer(),
                Serdes.String().deserializer());

        consumer.subscribe(Collections.singleton("test-topic"));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Long.MAX_VALUE);
            System.out.println("Pooled Records : " + consumerRecords.count());
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("Consumed Record with *******"+consumerRecord.key());
                upsert(consumerRecord);
            }
            System.out.println("Consumed Records : " + consumerRecords.count());
        }
    }


}
