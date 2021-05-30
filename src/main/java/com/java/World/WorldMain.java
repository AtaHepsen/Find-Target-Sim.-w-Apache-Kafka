package com.java.World;

import com.java.Processor.ProcessorMain;
import com.java.Sensor.Sensor;
import com.java.Sensor.SensorMain;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class WorldMain {

    public static char getInputOneOrTwo(){
        Scanner scanner = new Scanner(System.in);
        String decision = null;
        while(true){
            decision = scanner.nextLine();
            if(decision.equals("1") ||decision.equals("2"))
                break;
            else{
                System.out.println("Wrong input, please enter 1 or 2.");
                decision = scanner.nextLine();
            }
        }
        return decision.charAt(0);
    }

    public static int[] getCoordsInput(){
        Scanner scanner = new Scanner(System.in);
        int[] coordHolder = new int[2];
        while (true){
            System.out.println("Please enter x in range of [-500, 500]");
            try {
                coordHolder[0] = scanner.nextInt();
                if(coordHolder[0] >= -500 && coordHolder[0] <= 500)
                    break;
                else
                    throw new Exception();
            }catch (Exception exc){
                System.out.println("Wrong input, please enter x in range of [-500, 500]");
            }
        }
        while (true){
            System.out.println("Please enter y in range of [-500, 500]");
            try {
                coordHolder[1] = scanner.nextInt();
                if(coordHolder[1] >= -500 && coordHolder[1] <= 500)
                    break;
                else
                    throw new Exception();
            }catch (Exception exc){
                System.out.println("Wrong input, please enter y in range of (-500, 500)");
            }
        }
        return coordHolder;
    }

    public static String readProperties(){
        String url = null;
        while (url == null){
            try (InputStream input = WorldMain.class.getClassLoader().getResourceAsStream("config.properties")) {
                Properties prop = new Properties();
                if (input == null) {
                    System.out.println("Sorry, unable to find config.properties");
                    throw new Exception("Config file does not exist.");
                }
                prop.load(input);
                url = prop.getProperty("kafka.ip") +":"+prop.getProperty("kafka.port");
            } catch (IOException ex) {
                ex.printStackTrace();
            } catch (Exception e) {
                System.exit(-1);
            }
        }
        return url;
    }

    public static void runPrograms(char sensorDecision, int[] sensorOneCoords, int[] sensorTwoCoords){
        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    ProcessorMain.main(new String[0]);
                }
                catch(Exception exc){
                    System.out.println(exc);
                }
            }
        });
        t3.start();
        int[] finalSensorOneCoords = sensorOneCoords;
        int[] finalSensorTwoCoords = sensorTwoCoords;
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    if(sensorDecision == '1'){
                        String[] argsToPass = {Integer.toString(finalSensorOneCoords[0]), Integer.toString(finalSensorOneCoords[1])};
                        SensorMain.main(argsToPass);
                    }
                    else
                        SensorMain.main(new String[0]);
                }
                catch(Exception exc){
                    System.out.println(exc);
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    if(sensorDecision == '1'){
                        String[] argsToPass = {Integer.toString(finalSensorTwoCoords[0]), Integer.toString(finalSensorTwoCoords[1])};
                        SensorMain.main(argsToPass);
                    }
                    else
                        SensorMain.main(new String[0]);
                }
                catch(Exception exc){
                    System.out.println(exc);
                }
            }
        });
        t1.start();
        t2.start();
    }

    public static void main(String[] args) throws InterruptedException {
        int[] sensorOneCoords = new int[2];
        int[] sensorTwoCoords = new int[2];
        System.out.println("Do you want to place sensors or let them be placed randomly?");
        System.out.println("Please enter 1 to place yourself, enter 2 to place randomly.");
        char sensorDecision = getInputOneOrTwo();
        if(sensorDecision == '1'){
            System.out.println("Sensor 1;");
            sensorOneCoords = getCoordsInput();
            System.out.println("Sensor 2;");
            sensorTwoCoords = getCoordsInput();
        }
        runPrograms(sensorDecision, sensorOneCoords, sensorTwoCoords);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, readProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);
        int worldCounter = 0;

        while(true){
            Thread.sleep(5000);
            BitSet bitSet = new BitSet(1000001);
            Random random = new Random();
            int x = random.nextInt(1001);
            int y = random.nextInt(1001);
            int indexOfTarget = (y*1000)+x;
            x = x - 500;
            y = y - 500;
            System.out.println("From WORLD:     World ID: "+worldCounter+ " Target Coords: "+"("+x+", "+y+")");
            bitSet.set(indexOfTarget, true);
            bitSet.set(bitSet.size()-1, true);
            ProducerRecord producerRecord = new ProducerRecord("WorldData",Integer.toString(worldCounter),bitSet.toByteArray());
            producer.send(producerRecord);
            producer.flush();
            worldCounter++;
        }
    }
    
}
