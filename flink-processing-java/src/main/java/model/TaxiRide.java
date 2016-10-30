package model;

import java.io.Serializable;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class TaxiRide implements Serializable{

	private static final long serialVersionUID = 1997606141796430345L;
	
	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

	public TaxiRide() {}

	public TaxiRide(long rideId, boolean isStart, DateTime startTime, DateTime endTime,
					float startLon, float startLat, float endLon, float endLat,
					short passengerCnt) {

		this.rideId = rideId;
		this.isStart = isStart;
		this.startTime = startTime;
		this.endTime = endTime;
		this.startLon = startLon;
		this.startLat = startLat;
		this.endLon = endLon;
		this.endLat = endLat;
		this.passengerCnt = passengerCnt;
	}

	public long rideId;
	public boolean isStart;
	public DateTime startTime;
	public DateTime endTime;
	public float startLon;
	public float startLat;
	public float endLon;
	public float endLat;
	public short passengerCnt;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(rideId).append(",");
		sb.append(isStart ? "START" : "END").append(",");
		if (isStart) {
			sb.append(startTime.toString(timeFormatter)).append(",");
			sb.append(endTime.toString(timeFormatter)).append(",");
		} else {
			sb.append(endTime.toString(timeFormatter)).append(",");
			sb.append(startTime.toString(timeFormatter)).append(",");
		}
		sb.append(startLon).append(",");
		sb.append(startLat).append(",");
		sb.append(endLon).append(",");
		sb.append(endLat).append(",");
		sb.append(passengerCnt);

		return sb.toString();
	}

	public static TaxiRide fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 9) {
			throw new RuntimeException("Invalid record: " + line);
		}

		TaxiRide ride = new TaxiRide();

		try {
			ride.rideId = Long.parseLong(tokens[0]);

			switch (tokens[1]) {
				case "START":
					ride.isStart = true;
					ride.startTime = DateTime.parse(tokens[2], timeFormatter);
					ride.endTime = DateTime.parse(tokens[3], timeFormatter);
					break;
				case "END":
					ride.isStart = false;
					ride.endTime = DateTime.parse(tokens[2], timeFormatter);
					ride.startTime = DateTime.parse(tokens[3], timeFormatter);
					break;
				default:
					throw new RuntimeException("Invalid record: " + line);
			}

			ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
			ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
			ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
			ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
			ride.passengerCnt = Short.parseShort(tokens[8]);

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return ride;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiRide &&
				this.rideId == ((TaxiRide) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int)this.rideId;
	}

}

