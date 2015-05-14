package utils.firstProcess;

import java.util.HashMap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONValue;

import scala.Tuple2;

public class Json implements PairFunction {

	@SuppressWarnings("unchecked")
	@Override
	public Tuple2 call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		Tuple2 t = (Tuple2) arg0;
		Object key = t._1;
		String message = (String) t._2;

		HashMap<String, Object> event = (HashMap<String, Object>) JSONValue
				.parse(message);

		return new Tuple2(key, event);

	}
}
