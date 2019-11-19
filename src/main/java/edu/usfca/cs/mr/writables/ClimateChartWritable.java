package edu.usfca.cs.mr.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ClimateChartWritable implements Writable {
    private Text UTC_DATE;
    private Text GEOHASH;
    private FloatWritable LONGITUDE;
    private FloatWritable LATITUDE;
    private FloatWritable AIR_TEMPERATURE;
    private FloatWritable PRECIPITATION;
    private FloatWritable MAX_TEMPERATURE;
    private FloatWritable MIN_TEMPERATURE;
    private FloatWritable AVG_TEMPERATURE;
    private FloatWritable AVG_PRECIPITATION;

    public ClimateChartWritable() {
    	this.GEOHASH = new Text();
        this.UTC_DATE = new Text();
        this.LONGITUDE = new FloatWritable();
        this.LATITUDE = new FloatWritable();
        this.AIR_TEMPERATURE = new FloatWritable();
        this.PRECIPITATION = new FloatWritable();
        this.MAX_TEMPERATURE = new FloatWritable();
        this.MIN_TEMPERATURE = new FloatWritable();
        this.AVG_TEMPERATURE = new FloatWritable();
        this.AVG_PRECIPITATION = new FloatWritable();
    }

    public ClimateChartWritable(
    		Text GEOHASH, Text UTC_DATE, Text UTC_TIME, FloatWritable LONGITUDE, 
    		FloatWritable LATITUDE, FloatWritable AIR_TEMPERATURE, FloatWritable PRECIPITATION,
    		FloatWritable MAX_TEMPERATURE, FloatWritable MIN_TEMPERATURE,
    	    FloatWritable AVG_TEMPERATURE, FloatWritable AVG_PRECIPITATION) {
        this.GEOHASH = GEOHASH;
    	this.UTC_DATE = UTC_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
        this.MAX_TEMPERATURE = MAX_TEMPERATURE;
        this.MIN_TEMPERATURE = MIN_TEMPERATURE;
        this.AVG_TEMPERATURE = AVG_TEMPERATURE;
        this.AVG_PRECIPITATION = AVG_PRECIPITATION;
    }


    public void set(
    		Text GEOHASH, Text UTC_DATE, FloatWritable LONGITUDE, 
    		FloatWritable LATITUDE, FloatWritable AIR_TEMPERATURE, FloatWritable PRECIPITATION,
    		FloatWritable MAX_TEMPERATURE, FloatWritable MIN_TEMPERATURE,
    	    FloatWritable AVG_TEMPERATURE, FloatWritable AVG_PRECIPITATION) {
        this.GEOHASH = GEOHASH;
    	this.UTC_DATE = UTC_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
        this.MAX_TEMPERATURE = MAX_TEMPERATURE;
        this.MIN_TEMPERATURE = MIN_TEMPERATURE;
        this.AVG_TEMPERATURE = AVG_TEMPERATURE;
        this.AVG_PRECIPITATION = AVG_PRECIPITATION;
    }
    
    
    public void set(
    		Text GEOHASH, Text UTC_DATE, FloatWritable AIR_TEMPERATURE, FloatWritable PRECIPITATION) {
        this.GEOHASH = GEOHASH;
    	this.UTC_DATE = UTC_DATE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	GEOHASH.write(out);
    	UTC_DATE.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        AIR_TEMPERATURE.write(out);
        PRECIPITATION.write(out);
        MAX_TEMPERATURE.write(out);
        MIN_TEMPERATURE.write(out);
        AVG_PRECIPITATION.write(out);
        AVG_TEMPERATURE.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	GEOHASH.readFields(in);
    	UTC_DATE.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        AIR_TEMPERATURE.readFields(in);
        PRECIPITATION.readFields(in);
        MAX_TEMPERATURE.readFields(in);
        MIN_TEMPERATURE.readFields(in);
        AVG_TEMPERATURE.readFields(in);
        AVG_PRECIPITATION.readFields(in);
    }
    
    public Text getGEOHASH() {
        return GEOHASH;
    }

    public void setGEOHASH(Text GEOHASH) {
        this.GEOHASH = GEOHASH;
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

    public FloatWritable getAIR_TEMPERATURE() {
        return AIR_TEMPERATURE;
    }

    public void setAIR_TEMPERATURE(FloatWritable AIR_TEMPERATURE) {
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
    }

    public FloatWritable getPRECIPITATION() {
        return PRECIPITATION;
    }

    public void setPRECIPITATION(FloatWritable PRECIPITATION) {
        this.PRECIPITATION = PRECIPITATION;
    }
    
    public FloatWritable getMAX_TEMPERATURE() {
        return MAX_TEMPERATURE;
    }

    public void setMAX_TEMPERATURE(FloatWritable MAX_TEMPERATURE) {
        this.MAX_TEMPERATURE = MAX_TEMPERATURE;
    }
    
    public FloatWritable getMIN_TEMPERATURE() {
        return MIN_TEMPERATURE;
    }

    public void setMIN_TEMPERATURE(FloatWritable MIN_TEMPERATURE) {
        this.MIN_TEMPERATURE = MIN_TEMPERATURE;
    }
    
    public FloatWritable getAVG_TEMPERATURE() {
        return AVG_TEMPERATURE;
    }

    public void setAVG_TEMPERATURE(FloatWritable AVG_TEMPERATURE) {
        this.AVG_TEMPERATURE = AVG_TEMPERATURE;
    }
    
    public FloatWritable getAVG_PRECIPITATION() {
        return AVG_PRECIPITATION;
    }

    public void setAVG_PRECIPITATION(FloatWritable AVG_PRECIPITATION) {
        this.AVG_PRECIPITATION = AVG_PRECIPITATION;
    }
    
	@Override
	public String toString() {
		return  UTC_DATE + " " + MAX_TEMPERATURE + " " + MIN_TEMPERATURE + " " + AVG_PRECIPITATION + " " + AVG_TEMPERATURE;
	}
}
