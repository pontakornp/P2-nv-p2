package edu.usfca.cs.mr.writables;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SolarWindWritable implements Writable {
    private Text LST_DATE;
    private FloatWritable LONGITUDE;
    private FloatWritable LATITUDE;
    private FloatWritable PRECIPITATION;
    private FloatWritable SOLAR_RADIATION;
    private IntWritable SR_FLAG;
    private FloatWritable WIND_1_5;
    private IntWritable WIND_FLAG;


    public SolarWindWritable() {
        this.LST_DATE = new Text();
        this.LONGITUDE = new FloatWritable();
        this.LATITUDE = new FloatWritable();
        this.PRECIPITATION = new FloatWritable();
        this.SOLAR_RADIATION = new FloatWritable();
        this.SR_FLAG = new IntWritable();
        this.WIND_1_5 = new FloatWritable();
        this.WIND_FLAG = new IntWritable();
    }

    public SolarWindWritable(Text LST_DATE, FloatWritable LONGITUDE, FloatWritable LATITUDE, FloatWritable PRECIPITATION, FloatWritable SOLAR_RADIATION, IntWritable SR_FLAG, FloatWritable WIND_1_5, IntWritable WIND_FLAG) {
        this.LST_DATE = LST_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.PRECIPITATION = PRECIPITATION;
        this.SOLAR_RADIATION = SOLAR_RADIATION;
        this.SR_FLAG = SR_FLAG;
        this.WIND_1_5 = WIND_1_5;
        this.WIND_FLAG = WIND_FLAG;
    }


    public void set(Text LST_DATE, FloatWritable LONGITUDE, FloatWritable LATITUDE, FloatWritable PRECIPITATION, FloatWritable SOLAR_RADIATION, IntWritable SR_FLAG, FloatWritable WIND_1_5, IntWritable WIND_FLAG) {
        this.LST_DATE = LST_DATE;
        this.LONGITUDE = LONGITUDE;
        this.LATITUDE = LATITUDE;
        this.PRECIPITATION = PRECIPITATION;
        this.SOLAR_RADIATION = SOLAR_RADIATION;
        this.SR_FLAG = SR_FLAG;
        this.WIND_1_5 = WIND_1_5;
        this.WIND_FLAG = WIND_FLAG;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LST_DATE.write(out);
        LONGITUDE.write(out);
        LATITUDE.write(out);
        PRECIPITATION.write(out);
        SOLAR_RADIATION.write(out);
        SR_FLAG.write(out);
        WIND_1_5.write(out);
        WIND_FLAG.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LST_DATE.readFields(in);
        LONGITUDE.readFields(in);
        LATITUDE.readFields(in);
        PRECIPITATION.readFields(in);
        SOLAR_RADIATION.readFields(in);
        SR_FLAG.readFields(in);
        WIND_1_5.readFields(in);
        WIND_FLAG.readFields(in);
    }

}
