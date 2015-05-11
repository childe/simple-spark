package utils.firstProcess;

import java.util.HashMap;

import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONValue;

import scala.Tuple2;

public class Json implements Function {

	@SuppressWarnings("unchecked")
	@Override
	public Object call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		Tuple2<String, String> event = (Tuple2<String, String>) arg0;

		return (HashMap<String, Object>) JSONValue.parse(event._2);

	}
}
