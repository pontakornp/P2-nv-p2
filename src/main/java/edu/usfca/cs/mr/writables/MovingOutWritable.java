package edu.usfca.cs.mr.writables;

import com.google.common.collect.Streams;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovingOutWritable implements Writable {
    private Text LST_DATE;
    private FloatWritable LONGITUDE;
    private FloatWritable LATITUDE;
    private DoubleWritable AIR_TEMPERATURE;
    private DoubleWritable PRECIPITATION;
    private DoubleWritable RELATIVE_HUMIDITY;
    private IntWritable RH_FLAG;

    public MovingOutWritable() {
        this.LST_DATE = new Text();
        this.LONGITUDE = new FloatWritable();
        this.LATITUDE = new FloatWritable();
        this.AIR_TEMPERATURE = new DoubleWritable();
        this.PRECIPITATION = new DoubleWritable();
        this.RELATIVE_HUMIDITY = new DoubleWritable();
        this.RH_FLAG = new IntWritable();
    }

    public MovingOutWritable(Text LST_DATE, FloatWritable LONGITUDE, FloatWritable LATITUDE, DoubleWritable AIR_TEMPERATURE, DoubleWritable PRECIPITATION, DoubleWritable RELATIVE_HUMIDITY, IntWritable RH_FLAG) {
        this.LST_DATE = LST_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
        this.RELATIVE_HUMIDITY = RELATIVE_HUMIDITY;
        this.RH_FLAG = RH_FLAG;
    }

    public void set(Text LST_DATE, FloatWritable LONGITUDE, FloatWritable LATITUDE, DoubleWritable AIR_TEMPERATURE, DoubleWritable PRECIPITATION, DoubleWritable RELATIVE_HUMIDITY, IntWritable RH_FLAG) {
        this.LST_DATE = LST_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
        this.RELATIVE_HUMIDITY = RELATIVE_HUMIDITY;
        this.RH_FLAG = RH_FLAG;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LST_DATE.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        AIR_TEMPERATURE.write(out);
        PRECIPITATION.write(out);
        RELATIVE_HUMIDITY.write(out);
        RH_FLAG.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LST_DATE.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        AIR_TEMPERATURE.readFields(in);
        PRECIPITATION.readFields(in);
        RELATIVE_HUMIDITY.readFields(in);
        RH_FLAG.readFields(in);
    }

    public Text getLST_DATE() {
        return LST_DATE;
    }

    public void setLST_DATE(Text LST_DATE) {
        this.LST_DATE = LST_DATE;
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

    public DoubleWritable getAIR_TEMPERATURE() {
        return AIR_TEMPERATURE;
    }

    public void setAIR_TEMPERATURE(DoubleWritable AIR_TEMPERATURE) {
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
    }

    public DoubleWritable getPRECIPITATION() {
        return PRECIPITATION;
    }

    public void setPRECIPITATION(DoubleWritable PRECIPITATION) {
        this.PRECIPITATION = PRECIPITATION;
    }

    public DoubleWritable getRELATIVE_HUMIDITY() {
        return RELATIVE_HUMIDITY;
    }

    public void setRELATIVE_HUMIDITY(DoubleWritable RELATIVE_HUMIDITY) {
        this.RELATIVE_HUMIDITY = RELATIVE_HUMIDITY;
    }

    public IntWritable getRH_FLAG() {
        return RH_FLAG;
    }

    public void setRH_FLAG(IntWritable RH_FLAG) {
        this.RH_FLAG = RH_FLAG;
    }
}
