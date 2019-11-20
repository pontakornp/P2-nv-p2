package edu.usfca.cs.mr.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CornProductionWritable implements Writable {
    private Text LST_DATE;
    private Text LST_TIME;
    private Text GEOHASH;
    private FloatWritable LONGITUDE;
    private FloatWritable LATITUDE;
    private FloatWritable AIR_TEMPERATURE;
    private FloatWritable PRECIPITATION;
    private FloatWritable AVG_TEMPERATURE;
    private FloatWritable AVG_PRECIPITATION;
    private FloatWritable MIN_DAY_TEMPERATURE;
    private FloatWritable MAX_DAY_TEMPERATURE;
    private FloatWritable MAX_NIGHT_TEMPERATURE;
    private FloatWritable MIN_NIGHT_TEMPERATURE;

    public CornProductionWritable() {
    	this.GEOHASH = new Text();
        this.LST_DATE = new Text();
        this.LST_TIME = new Text();
        this.LONGITUDE = new FloatWritable();
        this.LATITUDE = new FloatWritable();
        this.AIR_TEMPERATURE = new FloatWritable();
        this.PRECIPITATION = new FloatWritable();
        this.AVG_TEMPERATURE = new FloatWritable();
        this.AVG_PRECIPITATION = new FloatWritable();
        this.MIN_DAY_TEMPERATURE = new FloatWritable();
        this.MAX_DAY_TEMPERATURE = new FloatWritable();
        this.MAX_NIGHT_TEMPERATURE = new FloatWritable();
        this.MIN_NIGHT_TEMPERATURE = new FloatWritable();
    }
    
    public void set(Text GEOHASH, Text LST_DATE, Text LST_TIME, FloatWritable AIR_TEMPERATURE,
			FloatWritable PRECIPITATION) {
    	this.GEOHASH = GEOHASH;
    	this.LST_DATE = LST_DATE;
    	this.LST_TIME = LST_TIME;
    	this.AIR_TEMPERATURE = AIR_TEMPERATURE;
        this.PRECIPITATION = PRECIPITATION;
	}

    @Override
    public void write(DataOutput out) throws IOException {
    	GEOHASH.write(out);
    	LST_DATE.write(out);
    	LST_TIME.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        AIR_TEMPERATURE.write(out);
        PRECIPITATION.write(out);
        AVG_TEMPERATURE.write(out);
        AVG_PRECIPITATION.write(out);
        MAX_DAY_TEMPERATURE.write(out);
        MIN_DAY_TEMPERATURE.write(out);
        MAX_NIGHT_TEMPERATURE.write(out);
        MIN_NIGHT_TEMPERATURE.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	GEOHASH.readFields(in);
    	LST_DATE.readFields(in);
    	LST_TIME.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        AIR_TEMPERATURE.readFields(in);
        PRECIPITATION.readFields(in);
        AVG_TEMPERATURE.readFields(in);
        AVG_PRECIPITATION.readFields(in);
        MAX_DAY_TEMPERATURE.readFields(in);
        MIN_DAY_TEMPERATURE.readFields(in);
        MAX_NIGHT_TEMPERATURE.readFields(in);
        MIN_NIGHT_TEMPERATURE.readFields(in);
    }
    
    public Text getGEOHASH() {
        return GEOHASH;
    }

    public void setGEOHASH(Text GEOHASH) {
        this.GEOHASH = GEOHASH;
    }
    
    public Text getLST_DATE() {
        return LST_DATE;
    }

    public void setLST_DATE(Text LST_DATE) {
        this.LST_DATE = LST_DATE;
    }
    
    public Text getLST_TIME() {
        return LST_TIME;
    }

    public void setLST_TIME(Text LST_TIME) {
        this.LST_TIME = LST_TIME;
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
    
    public FloatWritable getMAX_DAY_TEMPERATURE() {
        return MAX_DAY_TEMPERATURE;
    }

    public void setMAX_DAY_TEMPERATURE(FloatWritable MAX_DAY_TEMPERATURE) {
        this.MAX_DAY_TEMPERATURE = MAX_DAY_TEMPERATURE;
    }
    
    public FloatWritable getMIN_DAY_TEMPERATURE() {
        return MIN_DAY_TEMPERATURE;
    }

    public void setMIN_DAY_TEMPERATURE(FloatWritable MIN_DAY_TEMPERATURE) {
        this.MIN_DAY_TEMPERATURE = MIN_DAY_TEMPERATURE;
    }
    
    public FloatWritable getMAX_NIGHT_TEMPERATURE() {
        return MAX_NIGHT_TEMPERATURE;
    }

    public void setMAX_NIGHT_TEMPERATURE(FloatWritable MAX_NIGHT_TEMPERATURE) {
        this.MAX_NIGHT_TEMPERATURE = MAX_NIGHT_TEMPERATURE;
    }
    
    public FloatWritable getMIN_NIGHT_TEMPERATURE() {
        return MIN_NIGHT_TEMPERATURE;
    }

    public void setMIN_NIGHT_TEMPERATURE(FloatWritable MIN_NIGHT_TEMPERATURE) {
        this.MIN_NIGHT_TEMPERATURE = MIN_NIGHT_TEMPERATURE;
    }
    
	@Override
	public String toString() {
		return  LST_DATE + " " + MAX_DAY_TEMPERATURE + " " + MIN_DAY_TEMPERATURE + " " +
				MAX_NIGHT_TEMPERATURE + " " + MIN_NIGHT_TEMPERATURE + " " + 
				AVG_TEMPERATURE + " " + AVG_PRECIPITATION;
	}
}
