package edu.usfca.cs.mr.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


//    private Text WBANNO;
//    private Text UTC_DATE;
//    private Text UTC_TIME;
//    private Text LST_DATE;
//    private Text LST_TIME;
//    private IntWritable CRX_VN;
//    private DoubleWritable LONGITUDE;
//    private DoubleWritable LATITUDE;
//    private DoubleWritable AIR_TEMPERATURE;
//    private DoubleWritable PRECIPITATION;
//    private DoubleWritable SOLAR_RADIATION;
//    private IntWritable SR_FLAG;
//    private DoubleWritable SURFACE_TEMPERATURE;
//    private Text ST_TYPE;
//    private IntWritable ST_FLAG;
//    private DoubleWritable RELATIVE_HUMIDITY;
//    private IntWritable RH_FLAG;
//    private DoubleWritable SOIL_MOISTURE_5;
//    private DoubleWritable SOIL_TEMPERATURE_5;
//    private DoubleWritable WETNESS;
//    private IntWritable WET_FLAG;
//    private DoubleWritable WIND_1_5;
//    private IntWritable WIND_FLAG;

public class NcdcConstants {
    public static final int WBANNO_INDEX = 0;
    public static final int UTC_DATE_INDEX = 1 ;
    public static final int UTC_TIME_INDEX = 2;
    public static final int LST_DATE_INDEX = 3;
    public static final int LST_TIME_INDEX = 4;
    public static final int CRX_VN_INDEX = 5;
    public static final int LONGITUDE_INDEX = 6;
    public static final int LATITUDE_INDEX = 7;
    public static final int AIR_TEMPERATURE_INDEX = 8;
    public static final int PRECIPITATION_INDEX = 9;
    public static final int SOLAR_RADIATION_INDEX = 10;
    public static final int SR_FLAG_INDEX = 11;
    public static final int SURFACE_TEMPERATURE_INDEX = 12;
    public static final int ST_TYPE_INDEX = 13;
    public static final int ST_FLAG_INDEX = 14;
    public static final int RELATIVE_HUMIDITY_INDEX = 15;
    public static final int RH_FLAG_INDEX = 16;
    public static final int SOIL_MOISTURE_5_INDEX = 17;
    public static final int SOIL_TEMPERATURE_5_INDEX = 18;
    public static final int WETNESS_INDEX = 19;
    public static final int WET_FLAG_INDEX = 20;
    public static final int WIND_1_5_INDEX = 21;
    public static final int WIND_FLAG_INDEX = 22;

    public static final double MISSING_DATA_1 = -9999.0;
    public static final double MISSING_DATA_2 = -99.000;

}
