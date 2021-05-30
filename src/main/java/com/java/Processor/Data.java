package com.java.Processor;

public class Data {
    private int[] sensor1Coords;
    private int[] sensor2Coords;
    private double sensor1Angle;
    private double sensor2Angle;
    private String worldID;


    public int[] getSensor1Coords() {
        return sensor1Coords;
    }

    public void setSensor1Coords(int[] sensor1Coords) {
        this.sensor1Coords = sensor1Coords;
    }

    public int[] getSensor2Coords() {
        return sensor2Coords;
    }

    public void setSensor2Coords(int[] sensor2Coords) {
        this.sensor2Coords = sensor2Coords;
    }

    public double getSensor1Angle() {
        return sensor1Angle;
    }

    public void setSensor1Angle(double sensor1Angle) {
        this.sensor1Angle = sensor1Angle;
    }

    public double getSensor2Angle() {
        return sensor2Angle;
    }

    public void setSensor2Angle(double sensor2Angle) {
        this.sensor2Angle = sensor2Angle;
    }

    public String getWorldID() {
        return worldID;
    }

    public void setWorldID(String worldID) {
        this.worldID = worldID;
    }
}
