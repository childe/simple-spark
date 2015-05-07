package transformation;

import org.apache.spark.api.java.function.Function;

import java.util.HashMap;

public class Mutate implements Function {
	@Override
	public Object call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		return event;
	}
}
