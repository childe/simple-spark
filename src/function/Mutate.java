package function;

import org.apache.spark.api.java.function.Function;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.tree.Node;

import java.util.HashMap;
import java.util.Map;

public class Mutate implements Function {
	@Override
	public Object call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		return event;
	}
	
	public static void main(String[] args) {
		long s = System.currentTimeMillis();
		
		Jinjava jinjava = new Jinjava();

		Map<String, Object> context = new HashMap<>();      
		context.put("name", "Jared");
		String template = "Hello, {% if name is defined %} {{name}} {% else %} world {% endif %}";

		for (int i = 0; i < 70000; i++) {

		    jinjava.renderForResult(template, context);

		}       
		System.out.println(System.currentTimeMillis()-s);
	}
}
