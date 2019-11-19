package edu.usfca.cs.mr.util;

public class SolarWindOutput {
	
	public SolarWindOutput() {
		
	}
	
	public float solarRadiationSum;
	public int solarRadaitionCount;
	public float windSpeedSum;
	public int windSpeedCount;
	public float airTemperatureSum;
	public int airTemperatureCount;
	public float precipitationSum;
	public int precipitationCount;
	
	@Override
	public String toString() {
		return "SolarWindOutput [solarRadiationSum=" + solarRadiationSum + ", solarRadaitionCount="
				+ solarRadaitionCount + ", windSpeedSum=" + windSpeedSum + ", windSpeedCount=" + windSpeedCount
				+ ", airTemperatureSum=" + airTemperatureSum + ", airTemperatureCount=" + airTemperatureCount
				+ ", precipitationSum=" + precipitationSum + ", precipitationCount=" + precipitationCount + "]";
	}
	
	
}
