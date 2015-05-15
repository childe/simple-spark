package function;

import jinmanager.JinManager;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.hubspot.jinjava.interpret.Context;

import java.util.ArrayList;
import java.util.HashMap;

public class Setkey implements PairFunction {

	static public final String defaultTransformation = "mapToPair";
	static public final String defaultTransformationFunctionClass = "PairFunction";

	// private ArrayList<Node> keylist;
	private ArrayList<String> key;

	public Setkey(HashMap<String, Object> conf) {
		System.out.println(conf);
		this.key = (ArrayList<String>) conf.get("key");

		// this.keylist = new ArrayList<Node>();
		//
		// ArrayList<String> key = (ArrayList<String>) conf.get("key");
		// HashMap<String, Object> value = (HashMap<String, Object>) conf
		// .get("value");
		//
		// for (String template : key) {
		// TokenParser t = new TokenParser(null, template);
		// this.keylist.add(TreeParser.parseTree(t));
		// }
	}

	@Override
	public Tuple2 call(Object arg0) throws Exception {
		
		Tuple2 t = (Tuple2) arg0;
		final HashMap<String, Object> event = (HashMap<String, Object>) t._2;
		
		ArrayList<String> key = new ArrayList<String>();

		HashMap binding = new HashMap() {
			{
				put("event", event);
			}
		};
		Context cc = new Context(JinManager.c, binding);

		for (String _key : this.key) {
			key.add(JinManager.jinjava.render(_key, cc));
		}

		return new Tuple2(key, event);
	}

	public static void main(String[] args) throws ClassNotFoundException {
		Class<?>[] ifs = Class.forName("function.Setkey").getInterfaces();
		for (Class<?> class1 : ifs) {
			System.out.println(class1);
		}
	}
}
