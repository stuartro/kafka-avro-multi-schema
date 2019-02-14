package com.github.amitjn1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.testing.entity.*;

import java.util.Properties;

public class MultiAvroSchema {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        //Set value for new property
        properties.setProperty("value.subject.name.strategy", io.confluent.kafka.serializers.subject.TopicRecordNameStrategy.class.getName());
        //properties.setProperty("value.subject.name.strategy", io.confluent.kafka.serializers.subject.RecordNameStrategy.class.getName());

        String topic = "mykafkatopic1";

        //Block 1 start
        Producer<String, User> producer = new KafkaProducer<String, User>(properties);

        // copied from avro examples
        User user = User.newBuilder()
                .setFirstName("John5")
                .setLastName("Doe5")
                .build();

        ProducerRecord<String, User> producerRecord = new ProducerRecord<String, User>(
                topic, user
        );

        System.out.println(user);
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });
        producer.flush();
        producer.close();

//Block 1 end
//Block 2 start
        Producer<String, Movie> producer1 = new KafkaProducer<String, Movie>(properties);

        // copied from avro examples
        Movie movie = Movie.newBuilder()
                .setMovieName("Gladiator")
                .setGenre("History")
                .build();

        ProducerRecord<String, Movie> producerRecord1 = new ProducerRecord<String, Movie>(
                topic, movie
        );

        System.out.println(movie);
        producer1.send(producerRecord1, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });
        producer1.flush();
        producer1.close();
//Block 2 end

    }
}
