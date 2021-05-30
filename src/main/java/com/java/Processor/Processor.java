package com.java.Processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class Processor {
    private String topic = "SensorData";
    private String KAFKA_BROKERS;
    private KafkaConsumer<String, String> consumer;
    private HashMap<String, Data> dataDict;

    public Processor(){
        while(KAFKA_BROKERS == null)
            readProperties();
        initiateConsumer();
        dataDict = new HashMap<>();
        startProcessor();
    }

    public void startProcessor(){
        while (true){
            checkForSensorInput();
        }
    }

    public void checkForSensorInput(){
        ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(1000));
        for(ConsumerRecord consumerRecord: record){
            insertData(consumerRecord.key(), consumerRecord.value());
        }
    }

    public void insertData(Object key, Object value){
        if(key instanceof String && value instanceof  String){
            String valHolder = (String) value;
            if (valHolder.equals("0"))
                return;
            String[] valueData = parseValueToData((String) value);
            int[] keyData = parseKeyToCoord((String) key);
            Data relatedData = dataDict.containsKey(valueData[0]) ? dataDict.get(valueData[0]) : new Data();
            if (relatedData.getWorldID() == null){
                relatedData.setWorldID(valueData[0]);
                dataDict.put(valueData[0], relatedData);
            }
            if (relatedData.getSensor1Coords() == null){
                relatedData.setSensor1Coords(keyData);
                relatedData.setSensor1Angle(Double.parseDouble(valueData[1]));
            }
            else if(relatedData.getSensor2Coords() == null){
                relatedData.setSensor2Coords(keyData);
                relatedData.setSensor2Angle(Double.parseDouble(valueData[1]));
                findTargetCoords(relatedData);
            }
        }
    }

    public void findTargetCoords(Data data){
        //Find formulas for both lines as in y = mx + b
        //m is tan(angle)
        //find b with coordinates of sensor, for example given point 5,1 and slope m = 1
        // 1 = 1*5 + b
        // find b
        // do this for both
        // the mx + b = m'x + b'
        // this gets you x value for intersection
        // put x in one of the equations and calculate y
        // print x,y for the target point.
        double m1 = Math.tan(Math.toRadians(data.getSensor1Angle()));
        double m2 = Math.tan(Math.toRadians(data.getSensor2Angle()));
        double b1 = data.getSensor1Coords()[1] - (m1 * data.getSensor1Coords()[0]);
        double b2 = data.getSensor2Coords()[1] - (m2 * data.getSensor2Coords()[0]);
        double targetX = (b2-b1)/(m1-m2);
        double targetY = (m1 * targetX) + b1;
        System.out.println("From PROCESSOR: World ID: "+data.getWorldID()+" Target Coords: "+"("+Math.round(targetX)+", "+Math.round(targetY)+")");
    }

    public String[] parseValueToData(String value){
        String[] data = new String[2];
        int underscoreLoc = value.indexOf("_");
        // World ID
        data[0] = value.substring(0, underscoreLoc);
        // Angle
        data[1] = value.substring(underscoreLoc+1);
        return data;
    }

    public int[] parseKeyToCoord(String key){
        int[] coords = new int[2];
        int underscoreLoc = key.indexOf("_");
        coords[0] = Integer.parseInt(key.substring(0, underscoreLoc));
        coords[1] = Integer.parseInt(key.substring(underscoreLoc+1));
        return coords;
    }

    public void initiateConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toString(System.currentTimeMillis()));

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public void readProperties(){
        try (InputStream input = Processor.class.getClassLoader().getResourceAsStream("config.properties")) {
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
