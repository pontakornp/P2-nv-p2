package edu.usfca.cs.mr.solarwind;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.util.SolarWindOutput;
import edu.usfca.cs.mr.writables.SolarWindWritable;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class SolarWindReducer
extends Reducer<Text, SolarWindWritable, Text, SolarWindWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<SolarWindWritable> values, Context context)
    throws IOException, InterruptedException {
    	
    	String geoHash = key.toString();
    	SolarWindOutput solarWindOutput = new SolarWindOutput();
    	SolarWindWritable result = new SolarWindWritable();
    	for (SolarWindWritable val : values) {
    		float solarRadiation = Float.parseFloat(val.getSOLAR_RADIATION().toString());
    		int solarFlag = Integer.parseInt(val.getSR_FLAG().toString());
    		float windSpeed = Float.parseFloat(val.getWIND_1_5().toString());
    		int windFlag = Integer.parseInt(val.getWIND_FLAG().toString());
    		float airTemperature = Float.parseFloat(val.getAIR_TEMPERATURE().toString());
    		float precipitation = Float.parseFloat(val.getPRECIPITATION().toString());
    		
    		if(solarFlag == 0 && solarRadiation != NcdcConstants.MISSING_DATA_1 &&
    				solarRadiation != NcdcConstants.MISSING_DATA_2 && 
    				solarRadiation != NcdcConstants.MISSING_DATA_3) {
    			solarWindOutput.solarRadiationSum += solarRadiation;
    			solarWindOutput.solarRadaitionCount += 1;
    		}
    		
    		if (windFlag == 0 && windSpeed != NcdcConstants.MISSING_DATA_1 &&
    				windSpeed != NcdcConstants.MISSING_DATA_2) {
    			solarWindOutput.windSpeedSum += windSpeed;
    			solarWindOutput.windSpeedCount += 1;
    		}
    		
    		if(airTemperature != NcdcConstants.MISSING_DATA_1 &&
    				airTemperature != NcdcConstants.MISSING_DATA_2) {
    			solarWindOutput.airTemperatureSum += airTemperature;
    			solarWindOutput.airTemperatureCount += 1;
    		}
    		
    		if(precipitation != NcdcConstants.MISSING_DATA_1 &&
    				precipitation != NcdcConstants.MISSING_DATA_2) {
    			solarWindOutput.precipitationSum += precipitation;
    			solarWindOutput.precipitationCount += 1;
    		}
    	}
    	
    	result.setGEOHASH(new Text(geoHash));
    	if(solarWindOutput.solarRadaitionCount!=0) {
    		result.setSOLAR_RADIATION(new FloatWritable(solarWindOutput.solarRadiationSum/solarWindOutput.solarRadaitionCount));
    	}
    	if(solarWindOutput.windSpeedCount!=0) {
    		result.setWIND_1_5(new FloatWritable(solarWindOutput.windSpeedSum/solarWindOutput.windSpeedCount));
    	}
    	if(solarWindOutput.airTemperatureCount!=0) {
    		result.setAIR_TEMPERATURE(new FloatWritable(solarWindOutput.airTemperatureSum/solarWindOutput.airTemperatureCount));
    	}
    	if(solarWindOutput.precipitationCount!=0) {
    		result.setPRECIPITATION(new FloatWritable(solarWindOutput.precipitationSum/solarWindOutput.precipitationCount));
    	}
    	if(result!=null) {
    		context.write(key, result);
    	}
    }
}
