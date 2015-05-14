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

		float sum0 = 0;
		float sum1 = 0;

		if (event0.containsKey(this.sumkey)) {
			sum0 = (float) event0.get(this.sumkey);
		} else {
			try {
				sum0 = Float.parseFloat((String) event0.get(this.field));
			} catch (Exception e) {

			}
		}

		if (event1.containsKey(this.sumkey)) {
			sum1 = (float) event1.get(this.sumkey);
		} else {
			try {
				sum0 = Float.parseFloat((String) event1.get(this.field));
			} catch (Exception e) {

			}
		}

		final float sum = sum0 + sum1;

		return new HashMap<String, Object>() {
			{
				put(countkey, count0 + count1);
				put(sumkey, sum);
			}
		};
	}
}
