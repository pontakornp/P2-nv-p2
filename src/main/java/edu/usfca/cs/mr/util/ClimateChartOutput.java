package edu.usfca.cs.mr.util;

public class ClimateChartOutput {
	
	public float maxTemperature = Float.MIN_VALUE ;
	public float minTemperature = Float.MAX_VALUE;
	public float airTemperatureSum = 0;
	public int airTemperatureCount = 0 ;
	public float precipitationSum = 0;
	public int precipitationCount = 0;
	
	public ClimateChartOutput() {
		
	}

	@Override
	public String toString() {
		return "ClimateChartOutput [maxTemperature=" + maxTemperature + ", minTemperature=" + minTemperature
				+ ", airTemperatureSum=" + airTemperatureSum + ", airTemperatureCount=" + airTemperatureCount
				+ ", precipitationSum=" + precipitationSum + ", precipitationCount=" + precipitationCount
				+ ", getClass()=" + getClass() + ", hashCode()=" + hashCode() + ", toString()=" + super.toString()
				+ "]";
	}
}
