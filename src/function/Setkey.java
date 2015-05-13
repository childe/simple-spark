package function;

import jinfilter.Floor;
import jinmanager.JinManager;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.parse.TokenParser;
import com.hubspot.jinjava.tree.Node;
import com.hubspot.jinjava.tree.TreeParser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;
		ArrayList<String> key = new ArrayList<String>();

		HashMap binding = new HashMap();
		binding.put("event", event);
		Context cc = new Context(JinManager.c, binding);

		for (String _key : this.key) {
			key.add(JinManager.jinjava.render(_key, cc));
		}

		return new Tuple2(key, event);
	}

	public static void main(String[] args) {

	}
}
