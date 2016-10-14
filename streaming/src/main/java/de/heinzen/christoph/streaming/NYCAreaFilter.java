package de.heinzen.christoph.streaming;


import org.apache.flink.api.common.functions.FilterFunction;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

public class NYCAreaFilter implements FilterFunction<TaxiRide>{

	private static final long serialVersionUID = 2285424662913655378L;

	@Override
	public boolean filter(TaxiRide value) throws Exception {
		return GeoUtils.isInNYC(value.endLon, value.endLat) && GeoUtils.isInNYC(value.startLon, value.startLat);
	}

}
