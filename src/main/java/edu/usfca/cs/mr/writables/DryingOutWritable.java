package edu.usfca.cs.mr.writables;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DryingOutWritable implements Writable {
    private Text UTC_DATE;
    private FloatWritable LONGITUDE;
    private FloatWritable LATITUDE;
    private FloatWritable WETNESS;
    private IntWritable WET_FLAG;

    public DryingOutWritable() {
        this.UTC_DATE = new Text();
        this.LONGITUDE = new FloatWritable();
        this.LATITUDE = new FloatWritable();
        this.WETNESS = new FloatWritable();
        this.WET_FLAG = new IntWritable();
    }

    public DryingOutWritable(Text UTC_DATE, FloatWritable LONGITUDE, FloatWritable LATITUDE, FloatWritable WETNESS, IntWritable WET_FLAG) {
        this.UTC_DATE = UTC_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.WETNESS = WETNESS;
        this.WET_FLAG = WET_FLAG;
    }

    public void set(Text UTC_DATE, FloatWritable LONGITUDE, FloatWritable LATITUDE, FloatWritable WETNESS, IntWritable WET_FLAG) {
        this.UTC_DATE = UTC_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.WETNESS = WETNESS;
        this.WET_FLAG = WET_FLAG;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        UTC_DATE.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        WETNESS.write(out);
        WET_FLAG.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        UTC_DATE.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        WETNESS.readFields(in);
        WET_FLAG.readFields(in);
    }

    public Text getUTC_DATE() {
        return UTC_DATE;
    }

    public void setUTC_DATE(Text UTC_DATE) {
        this.UTC_DATE = UTC_DATE;
    }

    public FloatWritable getLONGITUDE() {
        return LONGITUDE;
    }

    public void setLONGITUDE(FloatWritable LONGITUDE) {
        this.LONGITUDE = LONGITUDE;
    }

    public FloatWritable getLATITUDE() {
        return LATITUDE;
    }

    public void setLATITUDE(FloatWritable LATITUDE) {
        this.LATITUDE = LATITUDE;
    }

    public FloatWritable getWETNESS() {
        return WETNESS;
    }

    public void setWETNESS(FloatWritable WETNESS) {
        this.WETNESS = WETNESS;
    }

    public IntWritable getWET_FLAG() {
        return WET_FLAG;
    }

    public void setWET_FLAG(IntWritable WET_FLAG) {
        this.WET_FLAG = WET_FLAG;
    }

    @Override
    public String toString() {
        return "DryingOutWritable{" +
                "WETNESS=" + WETNESS +
                '}';
    }
}
