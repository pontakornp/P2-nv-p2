package edu.usfca.cs.mr.cornproduction;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.util.CornProductionOutput;
import edu.usfca.cs.mr.util.CornProductionOutput.AnnualAirTemperature;
import edu.usfca.cs.mr.util.CornProductionOutput.AnnualPrecipitation;
import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.CornProductionWritable;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class CornProductionReducer
extends Reducer<Text, CornProductionWritable, Text, CornProductionWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<CornProductionWritable> values, Context context)
    throws IOException, InterruptedException {

    	CornProductionOutput cornProductionOutput = new CornProductionOutput();
    	CornProductionWritable result = new CornProductionWritable();
    	for (CornProductionWritable val : values) {
    		result.setGEOHASH(val.getGEOHASH());
    		
    		float airTemperature = Float.parseFloat(val.getAIR_TEMPERATURE().toString());
    		float precipitation = Float.parseFloat(val.getPRECIPITATION().toString());
    		String lstDate = val.getLST_DATE().toString();
    		Integer lstYear = Integer.parseInt(lstDate.substring(0, 4));
    		HashMap<Integer, AnnualAirTemperature> annualTemperature = cornProductionOutput.annualTemperature;
    		HashMap<Integer, AnnualPrecipitation> annualPrecipitation = cornProductionOutput.annualPrecipitation;
    		
    		int lstTime = Integer.parseInt(val.getLST_TIME().toString());
    		
    		if(!annualTemperature.containsKey(lstYear)) {
    			annualTemperature.put(lstYear, cornProductionOutput.new AnnualAirTemperature());
    		}
    		
    		if(!annualPrecipitation.containsKey(lstYear)) {
    			annualPrecipitation.put(lstYear, cornProductionOutput.new AnnualPrecipitation());
    		}
    		
    		if(airTemperature != NcdcConstants.MISSING_DATA_1 &&
    			airTemperature != NcdcConstants.MISSING_DATA_2 &&
    			airTemperature != NcdcConstants.MISSING_DATA_3) {
    			
    			if(lstTime>=NcdcConstants.MIN_DAY_TIME && lstTime<=NcdcConstants.MAX_DAY_TIME) {
    				cornProductionOutput.maxDayTemperature = Math.max(cornProductionOutput.maxDayTemperature, airTemperature);
    				cornProductionOutput.minDayTemperature = Math.min(cornProductionOutput.minDayTemperature, airTemperature);
    			} else {
    				cornProductionOutput.maxNightTemperature = Math.max(cornProductionOutput.maxNightTemperature, airTemperature);
    				cornProductionOutput.minNightTemperature = Math.min(cornProductionOutput.minNightTemperature, airTemperature);
    			}
    			
    			cornProductionOutput.annualTemperature.get(lstYear).airTemperatureSum += airTemperature;
    			cornProductionOutput.annualTemperature.get(lstYear).airTemperatureCount += 1;
    		}
    		
    		if(precipitation != NcdcConstants.MISSING_DATA_1 &&
    				precipitation != NcdcConstants.MISSING_DATA_2) {
    			cornProductionOutput.annualPrecipitation.get(lstYear).precipitationSum += precipitation;
    			cornProductionOutput.annualPrecipitation.get(lstYear).precipitationCount += 1;
    		}
    	}
    	System.out.println(cornProductionOutput);
    	result.setMAX_DAY_TEMPERATURE(new FloatWritable(cornProductionOutput.maxDayTemperature));
    	result.setMAX_NIGHT_TEMPERATURE(new FloatWritable(cornProductionOutput.maxNightTemperature));
    	result.setMIN_DAY_TEMPERATURE(new FloatWritable(cornProductionOutput.minDayTemperature));
    	result.setMIN_NIGHT_TEMPERATURE(new FloatWritable(cornProductionOutput.minNightTemperature));
    	
    	
    	for(Integer annualYear: cornProductionOutput.annualTemperature.keySet()) {
    		result.setLST_DATE(new Text(Integer.toString(annualYear)));
    		AnnualAirTemperature annualTempObj = cornProductionOutput.annualTemperature.get(annualYear);
    		if(annualTempObj.airTemperatureCount!=0) {
    			float avgAnnualTemperature = annualTempObj.airTemperatureSum/annualTempObj.airTemperatureCount;
    			result.setAVG_TEMPERATURE(new FloatWritable(avgAnnualTemperature));
    		}
    		
    		AnnualPrecipitation annualPrecipitationObj = cornProductionOutput.annualPrecipitation.get(annualYear);
    		if(annualPrecipitationObj.precipitationCount!=0) {
    			float avgAnnualPrecipitation = annualPrecipitationObj.precipitationSum/annualPrecipitationObj.precipitationCount;
    			result.setAVG_TEMPERATURE(new FloatWritable(avgAnnualPrecipitation));
    		}
    		if(result!=null) {
        		context.write(key, result);
        	}
    	}
    }
}
