package utils.firstProcess;

import java.util.HashMap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONValue;

import scala.Tuple2;

public class Plain implements PairFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2 call(Object arg0) {
		// TODO Auto-generated method stub
		Tuple2 t = (Tuple2) arg0;
		Object key = t._1;
		String message = (String) t._2;

		if (message == null) {
			message = "";
		}

		HashMap<String, Object> event = new HashMap<String, Object>();
		event.put("message", message);

		return new Tuple2(key, event);
	}
}
