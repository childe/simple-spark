package utils.firstProcess;

import java.util.HashMap;

import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONValue;

import scala.Tuple2;

public class Plain implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		String message = ((Tuple2<String, String>) arg0)._1();

		HashMap<String, Object> event = new HashMap<String, Object>();
		event.put("message", message);
		
		return event;

	}
}
