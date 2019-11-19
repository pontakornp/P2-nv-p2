package edu.usfca.cs.mr.writables;

import org.apache.hadoop.io.*;
import sun.misc.GC;
import sun.util.locale.LanguageTag;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ExtremesWritable implements Writable {
    private Text UTC_DATE;
    private Text UTC_TIME;
    private DoubleWritable LONGITUDE;
    private DoubleWritable LATITUDE;
    private DoubleWritable AIR_TEMPERATURE;
    private DoubleWritable SURFACE_TEMPERATURE;

    public ExtremesWritable() {
        this.UTC_DATE = new Text();
        this.UTC_TIME = new Text();
        this.LONGITUDE = new DoubleWritable();
        this.LATITUDE = new DoubleWritable();
        this.AIR_TEMPERATURE = new DoubleWritable();
        this.SURFACE_TEMPERATURE = new DoubleWritable();
    }

    public ExtremesWritable(Text UTC_DATE, Text UTC_TIME, DoubleWritable LONGITUDE, DoubleWritable LATITUDE, DoubleWritable AIR_TEMPERATURE, DoubleWritable SURFACE_TEMPERATURE) {
        this.UTC_DATE = UTC_DATE;
        this.UTC_TIME = UTC_TIME;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.SURFACE_TEMPERATURE = SURFACE_TEMPERATURE;
    }

    public void set(Text UTC_DATE, Text UTC_TIME, DoubleWritable LONGITUDE, DoubleWritable LATITUDE, DoubleWritable AIR_TEMPERATURE, DoubleWritable SURFACE_TEMPERATURE) {
        this.UTC_DATE = UTC_DATE;
        this.UTC_TIME = UTC_TIME;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.SURFACE_TEMPERATURE = SURFACE_TEMPERATURE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        UTC_DATE.write(out);
        UTC_TIME.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        AIR_TEMPERATURE.write(out);
        SURFACE_TEMPERATURE.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        UTC_DATE.readFields(in);
        UTC_TIME.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        AIR_TEMPERATURE.readFields(in);
        SURFACE_TEMPERATURE.readFields(in);
    }

    @Override
    public String toString() {
        return UTC_DATE + ", " + UTC_TIME  + ", " + LONGITUDE + ", " + LATITUDE + ", " + AIR_TEMPERATURE + ", " + SURFACE_TEMPERATURE;
    }

    public Text getUTC_DATE() {
        return UTC_DATE;
    }

    public void setUTC_DATE(Text UTC_DATE) {
        this.UTC_DATE = UTC_DATE;
    }

    public Text getUTC_TIME() {
        return UTC_TIME;
    }

    public void setUTC_TIME(Text UTC_TIME) {
        this.UTC_TIME = UTC_TIME;
    }

    public DoubleWritable getLONGITUDE() {
        return LONGITUDE;
    }

    public void setLONGITUDE(DoubleWritable LONGITUDE) {
        this.LONGITUDE = LONGITUDE;
    }

    public DoubleWritable getLATITUDE() {
        return LATITUDE;
    }

    public void setLATITUDE(DoubleWritable LATITUDE) {
        this.LATITUDE = LATITUDE;
    }

    public DoubleWritable getAIR_TEMPERATURE() {
        return AIR_TEMPERATURE;
    }

    public void setAIR_TEMPERATURE(DoubleWritable AIR_TEMPERATURE) {
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
    }

    public DoubleWritable getSURFACE_TEMPERATURE() {
        return SURFACE_TEMPERATURE;
    }

    public void setSURFACE_TEMPERATURE(DoubleWritable SURFACE_TEMPERATURE) {
        this.SURFACE_TEMPERATURE = SURFACE_TEMPERATURE;
    }
}
