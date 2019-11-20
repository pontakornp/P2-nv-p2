package edu.usfca.cs.mr.pcc;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.RunningStatisticsND;

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
public class PCCMapper
extends Mapper<LongWritable, Text, NullWritable, RunningStatisticsND> {
    private RunningStatisticsND runningStatisticsND = new RunningStatisticsND();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");
        
        int SR_FLAG_INT = Integer.parseInt(fields[NcdcConstants.SR_FLAG_INDEX]);
        int RH_FLAG_INT = Integer.parseInt(fields[NcdcConstants.RH_FLAG_INDEX]);
        int WET_FLAG_INT = Integer.parseInt(fields[NcdcConstants.WET_FLAG_INDEX]);
        int WIND_FLAG_INT = Integer.parseInt(fields[NcdcConstants.WIND_FLAG_INDEX]);
          
        // filter out the flag that does not represent good data
        if (SR_FLAG_INT != 0 || RH_FLAG_INT != 0 || WET_FLAG_INT != 0 || WIND_FLAG_INT != 0) {
        	return;
        }
        
        double AIR_TEMPERATURE_DOUBLE = Double.parseDouble(fields[NcdcConstants.AIR_TEMPERATURE_INDEX]);
        double PRECIPITATION_DOUBLE = Double.parseDouble(fields[NcdcConstants.PRECIPITATION_INDEX]);
        double SOLAR_RADIATION_DOUBLE = Double.parseDouble(fields[NcdcConstants.SOLAR_RADIATION_INDEX]);
        double SURFACE_TEMPERATURE = Double.parseDouble(fields[NcdcConstants.SURFACE_TEMPERATURE_INDEX]);
        double RELATIVE_HUMIDITY_DOUBLE = Double.parseDouble(fields[NcdcConstants.RELATIVE_HUMIDITY_INDEX]);
        double SOIL_MOISTURE_5_DOUBLE = Double.parseDouble(fields[NcdcConstants.SOIL_MOISTURE_5_INDEX]);
        double SOIL_TEMPERATURE_5_DOUBLE = Double.parseDouble(fields[NcdcConstants.SOIL_TEMPERATURE_5_INDEX]);
        double WETNESS_DOUBLE = Double.parseDouble(fields[NcdcConstants.WETNESS_INDEX]);
        double WIND_1_5_DOUBLE = Double.parseDouble(fields[NcdcConstants.WIND_1_5_INDEX]);
        
        // filter out missing data
        if (AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
        		PRECIPITATION_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
				SOLAR_RADIATION_DOUBLE == NcdcConstants.MISSING_DATA_1 ||        		
				SURFACE_TEMPERATURE == NcdcConstants.MISSING_DATA_1 ||	
				RELATIVE_HUMIDITY_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
				SOIL_MOISTURE_5_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
				SOIL_TEMPERATURE_5_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
				WETNESS_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
				WIND_1_5_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                
				AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
        		PRECIPITATION_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
				SOLAR_RADIATION_DOUBLE == NcdcConstants.MISSING_DATA_2 ||        		
				SURFACE_TEMPERATURE == NcdcConstants.MISSING_DATA_2 ||	
				RELATIVE_HUMIDITY_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
				SOIL_MOISTURE_5_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
				SOIL_TEMPERATURE_5_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
				WETNESS_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
				WIND_1_5_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                
				AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_3 ||
        		PRECIPITATION_DOUBLE == NcdcConstants.MISSING_DATA_3 ||
				SOLAR_RADIATION_DOUBLE == NcdcConstants.MISSING_DATA_3 ||        		
				SURFACE_TEMPERATURE == NcdcConstants.MISSING_DATA_3 ||	
				RELATIVE_HUMIDITY_DOUBLE == NcdcConstants.MISSING_DATA_3 ||
				SOIL_MOISTURE_5_DOUBLE == NcdcConstants.MISSING_DATA_3 ||
				SOIL_TEMPERATURE_5_DOUBLE == NcdcConstants.MISSING_DATA_3 ||
				WETNESS_DOUBLE == NcdcConstants.MISSING_DATA_3 ||
				WIND_1_5_DOUBLE == NcdcConstants.MISSING_DATA_3) {
        	return;  
        }
        
        // if air temperature is out of the range of the real air temperature recorded
        if (-90 > AIR_TEMPERATURE_DOUBLE || AIR_TEMPERATURE_DOUBLE > 60) {
        	return;
        }
        
        // if surface temperature is out of the range that is not realistic, return
        if (-250 > SURFACE_TEMPERATURE || SURFACE_TEMPERATURE > 250) {
        	return;
        }
        // if relative humidity percentage is not within 0 and 100, return
        if (0 > RELATIVE_HUMIDITY_DOUBLE || RELATIVE_HUMIDITY_DOUBLE > 100) {
        	return;
        }
        
        runningStatisticsND.put(AIR_TEMPERATURE_DOUBLE, PRECIPITATION_DOUBLE, SOLAR_RADIATION_DOUBLE, SURFACE_TEMPERATURE, RELATIVE_HUMIDITY_DOUBLE, SOIL_MOISTURE_5_DOUBLE, SOIL_TEMPERATURE_5_DOUBLE, WETNESS_DOUBLE, WIND_1_5_DOUBLE);
    }

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, RunningStatisticsND>.Context context)
			throws IOException, InterruptedException {
		System.out.println(runningStatisticsND.toString());
		context.write(null, runningStatisticsND);
	}
    
}
