package com.artursworld;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;



public class JsonToObejectTest {
	
	public static void main(String[] args) throws JSONException {
		String value = "[[100.0,338],[105.0,151],[110.0,100],[115.0,149],[120.0,84],[125.0,78],[130.0,68],[135.0,66],[140.0,119],[145.0,197],[150.0,163],[155.0,269],[160.0,417],[165.0,735],[170.0,805],[175.0,1121],[180.0,1873],[185.0,2904],[190.0,4235],[195.0,5529],[200.0,7334],[205.0,9769],[210.0,12704],[215.0,18115],[220.0,21249],[225.0,26287],[230.0,31914],[235.0,39017],[240.0,40763],[245.0,46361],[250.0,50192],[255.0,53152],[260.0,56207],[265.0,55960],[270.0,57749],[275.0,56210],[280.0,55269],[285.0,53791],[290.0,50482],[295.0,46808],[300.0,43672],[305.0,40188],[310.0,37640],[315.0,34167],[320.0,30735],[325.0,29333],[330.0,25793],[335.0,24057],[340.0,21973],[345.0,20142],[350.0,18013],[355.0,17475],[360.0,16243],[365.0,15387],[370.0,13542],[375.0,12968],[380.0,12638],[385.0,11408],[390.0,11546],[395.0,9744],[400.0,9612],[405.0,8343],[410.0,7979],[415.0,7395],[420.0,7489],[425.0,7063],[430.0,6460],[435.0,5812],[440.0,5306],[445.0,5670],[450.0,4987],[455.0,4623],[460.0,5064],[465.0,4381],[470.0,4232],[475.0,4427],[480.0,3666],[485.0,3382],[490.0,3187],[495.0,3018],[500.0,970]]";
		List<Tuple2<Double, Integer>> collectionList = getListByJSON(value);
		System.out.println(collectionList);
	}
	
	public static List<Tuple2<Double, Integer>> getListByJSON(String jsonString){    
		List<Tuple2<Double, Integer>> retList = new ArrayList<>();
		try {
				JSONArray transactionJSON = new JSONArray(jsonString);
				for(int i = 0 ; i< transactionJSON.length(); i++){
						JSONArray a = (JSONArray) transactionJSON.get(i);
						Double reactionTime = Double.parseDouble(a.get(0).toString());
						Integer userCount = Integer.parseInt(a.get(1).toString());
						retList.add(new Tuple2<Double, Integer>(reactionTime, userCount));
				}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retList;
	}
}
