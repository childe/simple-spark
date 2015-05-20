package utils.firstProcess;

import java.util.HashMap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONValue;

import scala.Tuple2;

public class Json implements PairFunction {

	@SuppressWarnings("unchecked")
	@Override
	public Tuple2 call(Object arg0) {
		// TODO Auto-generated method stub
		Tuple2 t = (Tuple2) arg0;
		Object key = t._1;
		final String message = (String) t._2;

		HashMap<String, Object> event = null;
		try {
			event = (HashMap<String, Object>) JSONValue.parse(message);
		} catch (Exception e) {
			event = new HashMap<String, Object>() {
				{
					put("message", message);
				}
			};
		}

		if (event == null) {
			event = new HashMap<String, Object>() {
				{
					put("message", message);
				}
			};
		}

		return new Tuple2(key, event);
	}
}
