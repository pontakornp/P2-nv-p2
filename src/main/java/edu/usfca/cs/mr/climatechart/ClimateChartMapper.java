package edu.usfca.cs.mr.climatechart;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.util.Coordinates;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.ClimateChartWritable;

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
public class ClimateChartMapper
extends Mapper<LongWritable, Text, Text, ClimateChartWritable> {
    private ClimateChartWritable ClimateChartWritable = new ClimateChartWritable();
    
    private Text GEOHASH = new Text();
    private Text UTC_DATE = new Text();
    private FloatWritable AIR_TEMPERATURE = new FloatWritable();
    private FloatWritable PRECIPITATION = new FloatWritable();
    private static final int GEOHASH_PRECISION = 4;
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");
        
        Configuration conf = context.getConfiguration();
        String GEOHASH_PARAM = conf.get("GEOHASH");
        System.out.println(GEOHASH_PARAM);

        String UTC_DATE_STRING = fields[NcdcConstants.UTC_DATE_INDEX];
        String UTC_MONTH_STRING = UTC_DATE_STRING.substring(4, 6);
        float LONGITUDE_DOUBLE = Float.parseFloat(fields[NcdcConstants.LONGITUDE_INDEX]);
        float LATITUDE_DOUBLE = Float.parseFloat(fields[NcdcConstants.LATITUDE_INDEX]);
        float AIR_TEMPERATURE_DOUBLE = Float.parseFloat(fields[NcdcConstants.AIR_TEMPERATURE_INDEX]);
        float PRECIPITATION_DOUBLE = Float.parseFloat(fields[NcdcConstants.PRECIPITATION_INDEX]);
        
        Coordinates COORDINATES = new Coordinates(LATITUDE_DOUBLE, LONGITUDE_DOUBLE);
    	String GEOHASH_STRING = Geohash.encode(COORDINATES, GEOHASH_PRECISION);
        
        GEOHASH.set(GEOHASH_STRING);
        UTC_DATE.set(UTC_DATE_STRING);
        AIR_TEMPERATURE.set(AIR_TEMPERATURE_DOUBLE);
        PRECIPITATION.set(PRECIPITATION_DOUBLE);
        
        ClimateChartWritable.set(GEOHASH, UTC_DATE, AIR_TEMPERATURE, PRECIPITATION);
        
        if (!(LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 || 
                LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2) && 
        		GEOHASH_PARAM.equals(GEOHASH_STRING)){
        	context.write(new Text(UTC_MONTH_STRING), ClimateChartWritable);
        }
    }
}
