package edu.usfca.cs.mr.solarwind;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.util.Coordinates;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.SolarWindWritable;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 *
 *
 * Field#  Name                           Units / Type
 * ---------------------------------------------------------------
 *    1    WBANNO                         XXXXX / STRING
 *    2    UTC_DATE                       YYYYMMDD / STRING
 *    3    UTC_TIME                       HHmm / STRING
 *    4    LST_DATE                       YYYYMMDD / STRING
 *    5    LST_TIME                       HHmm / STRING
 *    6    CRX_VN                         XXXXXX / INT
 *    7    LONGITUDE                      Decimal_/ degrees DOUBLE
 *    8    LATITUDE                       Decimal_degrees / DOUBLE
 *    9    AIR_TEMPERATURE                Celsius / DOUBLE
 *    10   PRECIPITATION                  mm / DOUBLE
 *    11   SOLAR_RADIATION                W/m^2 / DOUBLE
 *    12   SR_FLAG                        X / INT
 *    13   SURFACE_TEMPERATURE            Celsius / DOUBLE
 *    14   ST_TYPE                        X / STRING
 *    15   ST_FLAG                        X / INT
 *    16   RELATIVE_HUMIDITY              % / STRING
 *    17   RH_FLAG                        X / INT
 *    18   SOIL_MOISTURE_5                m^3/m^3 / DOUBLE
 *    19   SOIL_TEMPERATURE_5             Celsius / DOUBLE
 *    20   WETNESS                        Ohms / DOUBLE
 *    21   WET_FLAG                       X / INT
 *    22   WIND_1_5                       m/s / DOUBLE
 *    23   WIND_FLAG                      X / INT
 */
public class SolarWindMapper
extends Mapper<LongWritable, Text, Text, SolarWindWritable> {
    private SolarWindWritable solarWindWritable = new SolarWindWritable();
    
    private Text UTC_DATE = new Text();
    private Text UTC_TIME = new Text();
    private FloatWritable LONGITUDE = new FloatWritable();
    private FloatWritable LATITUDE = new FloatWritable();
    private FloatWritable AIR_TEMPERATURE = new FloatWritable();
    private FloatWritable PRECIPITATION = new FloatWritable();
    private FloatWritable SOLAR_RADIATION = new FloatWritable();
    private IntWritable SR_FLAG =  new IntWritable();
    private FloatWritable WIND_1_5 = new FloatWritable();
    private IntWritable WIND_FLAG = new IntWritable();
    private static final int GEOHASH_PRECISION = 4;
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");

        String UTC_DATE_STRING = fields[NcdcConstants.UTC_DATE_INDEX];
        String UTC_TIME_STRING = fields[NcdcConstants.UTC_TIME_INDEX];
        float LONGITUDE_DOUBLE = Float.parseFloat(fields[NcdcConstants.LONGITUDE_INDEX]);
        float LATITUDE_DOUBLE = Float.parseFloat(fields[NcdcConstants.LATITUDE_INDEX]);
        float AIR_TEMPERATURE_DOUBLE = Float.parseFloat(fields[NcdcConstants.AIR_TEMPERATURE_INDEX]);
        float PRECIPITATION_DOUBLE = Float.parseFloat(fields[NcdcConstants.PRECIPITATION_INDEX]);
        float SOLAR_RADIATION_DOUBLE = Float.parseFloat(fields[NcdcConstants.SOLAR_RADIATION_INDEX]);
        int  SR_FLAG_INT = Integer.parseInt(fields[NcdcConstants.SR_FLAG_INDEX]);
        float  WIND_1_5_DOUBLE = Float.parseFloat(fields[NcdcConstants.WIND_1_5_INDEX]);
        int WIND_FLAG_INT = Integer.parseInt(fields[NcdcConstants.WIND_FLAG_INDEX]);
        
        
        UTC_DATE.set(UTC_DATE_STRING);
        UTC_TIME.set(UTC_TIME_STRING);
        LONGITUDE.set(LONGITUDE_DOUBLE);
        LATITUDE.set(LATITUDE_DOUBLE);
        AIR_TEMPERATURE.set(AIR_TEMPERATURE_DOUBLE);
        PRECIPITATION.set(PRECIPITATION_DOUBLE);
        SOLAR_RADIATION.set(SOLAR_RADIATION_DOUBLE);
        SR_FLAG.set(SR_FLAG_INT);
        WIND_1_5.set(WIND_1_5_DOUBLE);
        WIND_FLAG.set(WIND_FLAG_INT);
        
        solarWindWritable.set(UTC_DATE, UTC_TIME, LONGITUDE, LATITUDE, AIR_TEMPERATURE, 
        		PRECIPITATION, SOLAR_RADIATION, SR_FLAG, WIND_1_5, WIND_FLAG);
        
        if (!(LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 || 
                LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2)){
        	
        	Coordinates coordinates = new Coordinates(LATITUDE_DOUBLE, LONGITUDE_DOUBLE);
        	String geoHash = Geohash.encode(coordinates, GEOHASH_PRECISION);
        	context.write(new Text(geoHash), solarWindWritable);
        }
    }
}
