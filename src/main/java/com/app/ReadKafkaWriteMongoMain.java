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

    //private static Logger logger = LogManager.getLogger(ReadKafkaWriteMongoMain.class.getName());


    private static MongoCollection<Document> mongoCollection;
    private static String hostName = "localhost";

    public static void main(String[] args) throws Exception{

        //logger.info("Application started ........");

        if(args != null && args.length > 0){
            hostName = args[0];
        }

        connectToMongo();
        consumeOutput(hostName+":19092");

    }


    private static void connectToMongo(){
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://"+hostName+":27017"));
        MongoDatabase database = mongoClient.getDatabase("Kafka_Mongo");
        mongoCollection = database.getCollection("test", Document.class)
                .withReadPreference(ReadPreference.primaryPreferred());
        System.out.println("connected to mongo");
        System.out.println("Record count in mongo : "+mongoCollection.count());
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
        System.out.println("Consumer called, connecting to : " + bootstrapServers);
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
            System.out.println("Starting pooling Records ...");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(2000);
            System.out.println("Pooled Records : " + consumerRecords.count());
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("Consumed Record with *******"+consumerRecord.key());
                upsert(consumerRecord);
            }
            System.out.println("Consumed Records : " + consumerRecords.count());
        }
    }


}
