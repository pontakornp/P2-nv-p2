package edu.usfca.cs.mr.util;

import java.util.HashMap;

public class CornProductionOutput {
	
	public class AnnualAirTemperature {
		public float airTemperatureSum = 0;
		public int airTemperatureCount = 0;
		
		@Override
		public String toString() {
			return "AnnualAirTemperature [airTemperatureSum=" + airTemperatureSum + ", airTemperatureCount="
					+ airTemperatureCount + "]";
		}
	}
	
	public class AnnualPrecipitation {
		public float precipitationSum = 0;
		public int precipitationCount = 0;
		
		@Override
		public String toString() {
			return "AnnualPrecipitation [precipitationSum=" + precipitationSum + ", precipitationCount="
					+ precipitationCount + "]";
		}
		
		
	}
	
	public float maxDayTemperature = Float.MIN_VALUE;
	public float minDayTemperature = Float.MAX_VALUE;
	
	public float maxNightTemperature = Float.MIN_VALUE;
	public float minNightTemperature = Float.MAX_VALUE;
	
	public HashMap<Integer, AnnualAirTemperature> annualTemperature = new HashMap<Integer, AnnualAirTemperature>();
	public HashMap<Integer, AnnualPrecipitation> annualPrecipitation = new HashMap<Integer, AnnualPrecipitation>();
	
	public CornProductionOutput() {
		
	}

	@Override
	public String toString() {
		return "CornProductionOutput [maxDayTemperature=" + maxDayTemperature + ", minDayTemperature="
				+ minDayTemperature + ", maxNightTemperature=" + maxNightTemperature + ", minNightTemperature="
				+ minNightTemperature + ", annualTemperature=" + annualTemperature + ", annualPrecipitation="
				+ annualPrecipitation + "]";
	}
}
