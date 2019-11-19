package edu.usfca.cs.mr.climatechart;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.util.ClimateChartOutput;
import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.ClimateChartWritable;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class ClimateChartReducer
extends Reducer<Text, ClimateChartWritable, Text, ClimateChartWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<ClimateChartWritable> values, Context context)
    throws IOException, InterruptedException {

    	ClimateChartOutput climateChartOutput = new ClimateChartOutput();
    	ClimateChartWritable result = new ClimateChartWritable();
    	for (ClimateChartWritable val : values) {
    		result.setGEOHASH(val.getGEOHASH());
    		
    		float airTemperature = Float.parseFloat(val.getAIR_TEMPERATURE().toString());
    		float precipitation = Float.parseFloat(val.getPRECIPITATION().toString());
    		
    		
    		if(airTemperature != NcdcConstants.MISSING_DATA_1 &&
    			airTemperature != NcdcConstants.MISSING_DATA_2 &&
    			airTemperature != NcdcConstants.MISSING_DATA_3) {
    			
    			climateChartOutput.maxTemperature = Math.max(climateChartOutput.maxTemperature, airTemperature);
    			climateChartOutput.minTemperature = Math.min(climateChartOutput.minTemperature, airTemperature);
    			climateChartOutput.airTemperatureSum += airTemperature;
    			climateChartOutput.airTemperatureCount += 1;
    		}
    		
    		if(precipitation != NcdcConstants.MISSING_DATA_1 &&
    				precipitation != NcdcConstants.MISSING_DATA_2) {
    			climateChartOutput.precipitationSum += precipitation;
    			climateChartOutput.precipitationCount += 1;
    		}
    	}
    	
    	if(climateChartOutput.airTemperatureCount!=0) {
    		result.setMAX_TEMPERATURE(new FloatWritable(climateChartOutput.maxTemperature));
    		result.setMIN_TEMPERATURE(new FloatWritable(climateChartOutput.minTemperature));
    		result.setAVG_TEMPERATURE(new FloatWritable(climateChartOutput.airTemperatureSum/climateChartOutput.airTemperatureCount));
    	}
    	
    	if(climateChartOutput.precipitationCount!=0) {
    		result.setAVG_PRECIPITATION(new FloatWritable(climateChartOutput.precipitationSum/climateChartOutput.precipitationCount));
    	}
    	
    	if(result!=null) {
    		context.write(key, result);
    	}
    }
}
