/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.java.Sensor;

import java.util.Random;

public class SensorMain {

    public static void main(String[] args) throws InterruptedException {
        Random rand = new Random();
        int[] coordinates = new int[2];
        coordinates[0] = rand.nextInt(1001) - 500;
        coordinates[1] = rand.nextInt(1001) - 500;
        if (args.length > 0){
            coordinates[0] = Integer.parseInt(args[0]);
            coordinates[1] = Integer.parseInt(args[1]);
        }
        System.out.println("Sensor loaded: "+"("+coordinates[0]+", "+coordinates[1]+")");
        Sensor sensor = new Sensor(coordinates[0], coordinates[1]);
    }
    
}
