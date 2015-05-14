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

public class Count2 implements Function2 {
	ArrayList<HashMap<String, Object>> convert;

	static public final String defaultTransformation = "reduceByKey";

	static public final String defaultTransformationFunctionClass = "Function2";

	public Count2() {
	}

	@SuppressWarnings("unchecked")
	public Count2(HashMap<String, Object> conf) {
		System.out.println(conf);
	}

	@Override
	public Object call(Object arg0, Object arg1) throws Exception {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		HashMap<String, Object> event0 = (HashMap<String, Object>) arg0;
		HashMap<String, Object> event1 = (HashMap<String, Object>) arg1;

		final int count0 = (int) (event0.containsKey("count") ? event0
				.get("count") : 1);
		final int count1 = (int) (event1.containsKey("count") ? event0
				.get("count") : 1);

		return new HashMap<String, Integer>() {
			{
				put("count", count0 + count1);
			}
		};
	}
}
