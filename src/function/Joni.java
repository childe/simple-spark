package function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import utils.joni.JoniManager;
import utils.postProcess.PostProcess;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

/*
 * we could config more than one match in this function;
 * same tagOnFailure applies to the event if one match fails.
 */
public class Joni implements PairFunction {

	private final static Logger LOGGER = Logger.getLogger(Joni.class.getName());

	static public final String defaultTransformation = "mapToPair";

	private String id;
	private Map conf;

	@SuppressWarnings("unchecked")
	public Joni(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.conf = conf;
		this.id = (String) conf.get("id");

	}

	public Tuple2 call(Object arg0) {
		Tuple2 t = (Tuple2) arg0;
		Object originKey = t._1;
		HashMap<String, Object> event = (HashMap<String, Object>) t._2;

		JoniManager.getInstance((String) this.id, this.conf).process(event);

		return new Tuple2(originKey, event);

	}

	public static void main(String[] args) {
	}
}
