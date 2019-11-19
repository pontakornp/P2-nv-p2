package edu.usfca.cs.mr.writables;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TravelStartupWritable implements Writable {

    private IntWritable monthNum;
    private Text geoHash;
    private DoubleWritable AIR_TEMPERATURE;
    private DoubleWritable RELATIVE_HUMIDITY;
    private DoubleWritable comfortIndex;

    public TravelStartupWritable() {
        this.monthNum = new IntWritable();
        this.geoHash = new Text();
        this.AIR_TEMPERATURE = new DoubleWritable();
        this.RELATIVE_HUMIDITY = new DoubleWritable();
        this.comfortIndex = new DoubleWritable();
    }

    public TravelStartupWritable(IntWritable monthNum, Text geoHash, DoubleWritable AIR_TEMPERATURE, DoubleWritable RELATIVE_HUMIDITY, DoubleWritable comfortIndex) {
        this.monthNum = monthNum;
        this.geoHash = geoHash;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.RELATIVE_HUMIDITY = RELATIVE_HUMIDITY;
        this.comfortIndex = comfortIndex;
    }

    public void set(IntWritable monthNum, Text geoHash, DoubleWritable AIR_TEMPERATURE, DoubleWritable RELATIVE_HUMIDITY, DoubleWritable comfortIndex) {
        this.monthNum = monthNum;
        this.geoHash = geoHash;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.RELATIVE_HUMIDITY = RELATIVE_HUMIDITY;
        this.comfortIndex = comfortIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        monthNum.write(out);
        geoHash.write(out);
        AIR_TEMPERATURE.write(out);
        RELATIVE_HUMIDITY.write(out);
        comfortIndex.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        monthNum.readFields(in);
        geoHash.readFields(in);
        AIR_TEMPERATURE.readFields(in);
        RELATIVE_HUMIDITY.readFields(in);
        comfortIndex.readFields(in);
    }

    public IntWritable getMonthNum() {
        return monthNum;
    }

    public void setMonthNum(IntWritable monthNum) {
        this.monthNum = monthNum;
    }

    public Text getGeoHash() {
        return geoHash;
    }

    public void setGeoHash(Text geoHash) {
        this.geoHash = geoHash;
    }

    public DoubleWritable getAIR_TEMPERATURE() {
        return AIR_TEMPERATURE;
    }

    public void setAIR_TEMPERATURE(DoubleWritable AIR_TEMPERATURE) {
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
    }

    public DoubleWritable getRELATIVE_HUMIDITY() {
        return RELATIVE_HUMIDITY;
    }

    public void setRELATIVE_HUMIDITY(DoubleWritable RELATIVE_HUMIDITY) {
        this.RELATIVE_HUMIDITY = RELATIVE_HUMIDITY;
    }

    public DoubleWritable getComfortIndex() {
        return comfortIndex;
    }

    public void setComfortIndex(DoubleWritable comfortIndex) {
        this.comfortIndex = comfortIndex;
    }

    @Override
    public String toString() {
        return monthNum + ", " + geoHash + ", " + AIR_TEMPERATURE + ", " + RELATIVE_HUMIDITY + ", " + comfortIndex;
    }
}
