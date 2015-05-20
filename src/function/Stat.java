package function;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.PUT;

import jinmanager.JinManager;

public class Stat implements Function2 {
	ArrayList<HashMap<String, Object>> convert;

	static public final String defaultTransformation = "reduceByKey";

	private final String field, countkey, sumkey;

	@SuppressWarnings("unchecked")
	public Stat(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.field = (String) conf.get("field");
		this.countkey = this.field + "_count";
		this.sumkey = this.field + "_sum";
	}

	@Override
	public Object call(Object arg0, Object arg1) throws Exception {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		HashMap<String, Object> event0 = (HashMap<String, Object>) arg0;
		HashMap<String, Object> event1 = (HashMap<String, Object>) arg1;

		final int count0 = (int) (event0.containsKey(this.countkey) ? event0
				.get(countkey) : 1);
		final int count1 = (int) (event1.containsKey(this.countkey) ? event1
				.get(countkey) : 1);

		double sum0 = 0;
		double sum1 = 0;

		if (event0.containsKey(this.sumkey)) {
			sum0 = (double) event0.get(this.sumkey);
		} else {
			try {
				sum0 = Double.parseDouble((String) event0.get(this.field));
			} catch (Exception e) {

			}
		}

		if (event1.containsKey(this.sumkey)) {
			sum1 = (double) event1.get(this.sumkey);
		} else {
			try {
				sum1 = Double.parseDouble((String) event1.get(this.field));
			} catch (Exception e) {

			}
		}

		final double sum = sum0 + sum1;

		return new HashMap<String, Object>() {
			{
				put(countkey, count0 + count1);
				put(sumkey, sum);
			}
		};
	}
	
	public static void main(String[] args){
		HashMap pair = new HashMap();
		ArrayList a1 = new ArrayList(){{
			add(1);add(2);
		}};
		ArrayList a2 = new ArrayList(){{
			add(1);add(2);add(3);
		}};
		
		pair.put(a1, 1);
		pair.put(a2, 2);
		
		System.out.println(pair.size());
		
	}
}
