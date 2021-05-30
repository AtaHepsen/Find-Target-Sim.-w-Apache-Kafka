package com.java.Sensor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Properties;

public class Sensor {
    private int x;
    private int y;
    private String topic = "SensorData";
    private String topicWorld = "WorldData";
    private String KAFKA_BROKERS;
    private KafkaConsumer<String, byte[]> consumer;
    private KafkaProducer producer;

    public Sensor(int x, int y){
        this.x = x;
        this.y = y;
        while (KAFKA_BROKERS == null)
            readProperties();
        initiateConsumer();
        initiateProducer();
        sendSelfCoord();
        startSensor();
    }

    public void startSensor(){
        while (true){
            checkForWorldData();
        }
    }

    public void checkForWorldData(){
        ConsumerRecords<String, byte[]> record = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord consumerRecord: record){
                BitSet bitSet = BitSet.valueOf((byte[]) consumerRecord.value());
                for(int i = 0;i < bitSet.size() && i < 1000001;i++){
                    if(bitSet.get(i))
                        sendTargetCoord(i, consumerRecord.key());
                }
            }
    }

    public void sendTargetCoord(int indexOfTarget, Object worldID){
        int x2 = indexOfTarget % 1000 - 500;
        int y2 = 500 - (int)(indexOfTarget / 1000);
        double angle = Math.atan2(y2 - y, x2 - x) * 180 / Math.PI;
        System.out.println("From SENSOR:    I found target with angle of "+ angle);
        ProducerRecord producerRecord = new ProducerRecord(topic, x +"_"+ y,worldID+"_"+ angle);
        producer.send(producerRecord);
    }

    public void sendSelfCoord(){
        ProducerRecord producerRecord = new ProducerRecord(topic, x +"_"+ y,"0");
        producer.send(producerRecord);
    }

    public void initiateProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer(props);
    }

    public void initiateConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, x +","+ y);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicWorld));
    }

    public void readProperties(){
        try (InputStream input = Sensor.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
            prop.load(input);
            KAFKA_BROKERS = prop.getProperty("kafka.ip") +":"+prop.getProperty("kafka.port");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
