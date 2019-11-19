package edu.usfca.cs.mr.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SolarWindWritable implements Writable {
    private Text UTC_DATE;
    private Text UTC_TIME;
    private FloatWritable LONGITUDE;
    private FloatWritable LATITUDE;
    private FloatWritable AIR_TEMPERATURE;
    private FloatWritable PRECIPITATION;
    private FloatWritable SOLAR_RADIATION;
    private IntWritable SR_FLAG;
    private FloatWritable WIND_1_5;
    private IntWritable WIND_FLAG;
    private Text GEOHASH;


    public SolarWindWritable() {
    	this.GEOHASH = new Text();
        this.UTC_DATE = new Text();
        this.UTC_TIME = new Text();
        this.LONGITUDE = new FloatWritable();
        this.LATITUDE = new FloatWritable();
        this.AIR_TEMPERATURE = new FloatWritable();
        this.PRECIPITATION = new FloatWritable();
        this.SOLAR_RADIATION = new FloatWritable();
        this.SR_FLAG = new IntWritable();
        this.WIND_1_5 = new FloatWritable();
        this.WIND_FLAG = new IntWritable();
        
    }

    public SolarWindWritable(
    		Text UTC_DATE, Text UTC_TIME, FloatWritable LONGITUDE, FloatWritable LATITUDE, FloatWritable AIR_TEMPERATURE, 
    		FloatWritable PRECIPITATION, FloatWritable SOLAR_RADIATION, IntWritable SR_FLAG, 
    		FloatWritable WIND_1_5, IntWritable WIND_FLAG) {
        this.UTC_DATE = UTC_DATE;
        this.UTC_TIME = UTC_TIME;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
        this.SOLAR_RADIATION = SOLAR_RADIATION;
        this.SR_FLAG = SR_FLAG;
        this.WIND_1_5 = WIND_1_5;
        this.WIND_FLAG = WIND_FLAG;
    }


    public void set(
    		Text UTC_DATE, Text UTC_TIME, FloatWritable LONGITUDE, FloatWritable LATITUDE, FloatWritable AIR_TEMPERATURE, 
    		FloatWritable PRECIPITATION, FloatWritable SOLAR_RADIATION, IntWritable SR_FLAG, 
    		FloatWritable WIND_1_5, IntWritable WIND_FLAG) {
        this.UTC_DATE = UTC_DATE;
        this.UTC_TIME = UTC_TIME;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
        this.SOLAR_RADIATION = SOLAR_RADIATION;
        this.SR_FLAG = SR_FLAG;
        this.WIND_1_5 = WIND_1_5;
        this.WIND_FLAG = WIND_FLAG;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	GEOHASH.write(out);
    	UTC_DATE.write(out);
    	UTC_TIME.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        AIR_TEMPERATURE.write(out);
        PRECIPITATION.write(out);
        SOLAR_RADIATION.write(out);
        SR_FLAG.write(out);
        WIND_1_5.write(out);
        WIND_FLAG.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	GEOHASH.readFields(in);
    	UTC_DATE.readFields(in);
    	UTC_TIME.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        AIR_TEMPERATURE.readFields(in);
        PRECIPITATION.readFields(in);
        SOLAR_RADIATION.readFields(in);
        SR_FLAG.readFields(in);
        WIND_1_5.readFields(in);
        WIND_FLAG.readFields(in);
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

    public Text getUTC_TIME() {
        return UTC_TIME;
    }

    public void setUTC_TIME(Text UTC_TIME) {
        this.UTC_TIME = UTC_TIME;
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
    
    public FloatWritable getSOLAR_RADIATION() {
        return SOLAR_RADIATION;
    }

    public void setSOLAR_RADIATION(FloatWritable SOLAR_RADIATION) {
        this.SOLAR_RADIATION = SOLAR_RADIATION;
    }
    
    public IntWritable getSR_FLAG() {
        return SR_FLAG;
    }

    public void setSR_FLAG(IntWritable SR_FLAG) {
        this.SR_FLAG = SR_FLAG;
    }
    
    public FloatWritable getWIND_1_5() {
        return WIND_1_5;
    }

    public void setWIND_1_5(FloatWritable WIND_1_5) {
        this.WIND_1_5 = WIND_1_5;
    }
    
    public IntWritable getWIND_FLAG() {
        return WIND_FLAG;
    }

    public void setWIND_FLAG(IntWritable WIND_FLAG) {
        this.WIND_FLAG = WIND_FLAG;
    }

	@Override
	public String toString() {
		return  AIR_TEMPERATURE + " " + PRECIPITATION
				+ "	" + SOLAR_RADIATION + " " + WIND_1_5;
	}
    
}
