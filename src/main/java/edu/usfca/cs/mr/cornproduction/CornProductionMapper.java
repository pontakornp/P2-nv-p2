package edu.usfca.cs.mr.cornproduction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.util.Coordinates;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.CornProductionWritable;

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
public class CornProductionMapper
extends Mapper<LongWritable, Text, Text, CornProductionWritable> {
    private CornProductionWritable CornProductionWritable = new CornProductionWritable();
    
    private Text GEOHASH = new Text();
    private Text LST_DATE = new Text();
    private Text LST_TIME = new Text();
    private FloatWritable AIR_TEMPERATURE = new FloatWritable();
    private FloatWritable PRECIPITATION = new FloatWritable();
    private static final int GEOHASH_PRECISION = 5;
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");
        Configuration conf = context.getConfiguration();
        
        String LST_DATE_STRING = fields[NcdcConstants.LST_DATE_INDEX];
        String LST_TIME_STRING = fields[NcdcConstants.LST_TIME_INDEX];
        
        float LONGITUDE_DOUBLE = Float.parseFloat(fields[NcdcConstants.LONGITUDE_INDEX]);
        float LATITUDE_DOUBLE = Float.parseFloat(fields[NcdcConstants.LATITUDE_INDEX]);
        float AIR_TEMPERATURE_DOUBLE = Float.parseFloat(fields[NcdcConstants.AIR_TEMPERATURE_INDEX]);
        float PRECIPITATION_DOUBLE = Float.parseFloat(fields[NcdcConstants.PRECIPITATION_INDEX]);        
        
        Coordinates COORDINATES = new Coordinates(LATITUDE_DOUBLE, LONGITUDE_DOUBLE);
    	String GEOHASH_STRING = Geohash.encode(COORDINATES, GEOHASH_PRECISION);
        
    	
    	Integer CONF_MINMONTH = Integer.parseInt(conf.get("MINMONTH"));
    	Integer CONF_MAXMONTH = Integer.parseInt(conf.get("MAXMONTH"));
    	Integer CONF_MINDATE = Integer.parseInt(conf.get("MINDATE"));
    	Integer CONF_MAXDATE = Integer.parseInt(conf.get("MAXDATE"));
        
        Integer LST_MONTH_INT = Integer.parseInt(LST_DATE_STRING.substring(4, 6));
        Integer LST_EXACT_DATE_STRING = Integer.parseInt(LST_DATE_STRING.substring(6, 8));
        
        boolean DATERANGE = (LST_MONTH_INT == CONF_MINMONTH && LST_EXACT_DATE_STRING > CONF_MINDATE) ||
        		(LST_MONTH_INT == CONF_MAXMONTH && LST_EXACT_DATE_STRING < CONF_MAXDATE) || 
        		(LST_MONTH_INT > CONF_MINMONTH && LST_MONTH_INT < CONF_MAXMONTH);
        
        GEOHASH.set(GEOHASH_STRING);
        LST_DATE.set(LST_DATE_STRING);
        LST_TIME.set(LST_TIME_STRING);
        AIR_TEMPERATURE.set(AIR_TEMPERATURE_DOUBLE);
        PRECIPITATION.set(PRECIPITATION_DOUBLE);
        
        CornProductionWritable.set(GEOHASH, LST_DATE, LST_TIME, AIR_TEMPERATURE, PRECIPITATION);
        
        if (!(LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 || 
                LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2) && DATERANGE){
        	context.write(new Text(GEOHASH_STRING), CornProductionWritable);
        }
    }
}
