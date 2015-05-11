import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import transformation.Date;
import transformation.Split;
import utils.firstProcess.Json;
import utils.firstProcess.Plain;

import org.apache.spark.SparkConf;

//import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.yaml.snakeyaml.Yaml;

import com.twitter.chill.Base64.InputStream;

public class DoIT {
	@SuppressWarnings({ "unchecked" })
	public static void parseInputs(HashMap<String, Object> streams,
			JavaStreamingContext jssc, HashMap<String, Object> inputs) {

		System.out.println(inputs);
		Iterator<Entry<String, Object>> inputsIT = inputs.entrySet().iterator();
		while (inputsIT.hasNext()) {
			Map.Entry<String, Object> input = inputsIT.next();
			String inputType = (String) input.getKey();

			if (inputType.equalsIgnoreCase("kafka")) {
				HashMap<String, Object> inputsWithCertainType = (HashMap<String, Object>) input
						.getValue();

				Iterator<Entry<String, Object>> inputsWithCertainTypeIT = inputsWithCertainType
						.entrySet().iterator();
				while (inputsWithCertainTypeIT.hasNext()) {
					Map.Entry<String, Object> oneInputWithCertainTypeEntry = inputsWithCertainTypeIT
							.next();

					String streamID = (String) oneInputWithCertainTypeEntry
							.getKey();

					// kafaf streaming
					HashMap<String, Object> oneInputConfigWithCertainType = (HashMap<String, Object>) oneInputWithCertainTypeEntry
							.getValue();
					HashMap<String, Integer> topicsMap = (HashMap<String, Integer>) oneInputConfigWithCertainType
							.get("topics");


					String zkQuorum = (String) oneInputConfigWithCertainType
							.get("zookeeper");
					String group = (String) oneInputConfigWithCertainType
							.get("groupID");

					// put message to event
					String codec = "json";
					if (oneInputConfigWithCertainType.containsKey("codec")) {
						codec = (String) oneInputConfigWithCertainType
								.get("codec");
					}

					JavaPairReceiverInputDStream<String, String> a = KafkaUtils
							.createStream(jssc, zkQuorum, group, topicsMap);

					if (codec.equalsIgnoreCase("json")) {
						streams.put(streamID, a.map(new Json()));
					} else if (codec.equalsIgnoreCase("plain")) {
						streams.put(streamID, a.map(new Plain()));
					}

				}
			}
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// prepare configuration

		Map<String, Object> topologyConf = null;
		Yaml yaml = new Yaml();
		FileInputStream input;
		try {
			input = new FileInputStream(new File(args[0]));
			topologyConf = (Map<String, Object>) yaml.load(input);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(topologyConf);

		// spark conf

		String appName = (String) topologyConf.get("app_name");
		SparkConf sparkConf = new SparkConf().setAppName(appName);

		HashMap<String, Object> spark_conf = (HashMap<String, Object>) topologyConf
				.get("spark_conf");
		Iterator<Entry<String, Object>> it = spark_conf.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Object> entry = it.next();
			sparkConf.set(entry.getKey(), (String) entry.getValue());
		}

		// build streams

		HashMap<String, Object> streams = new HashMap<String, Object>();

		// input

		HashMap<String, Object> inputsConfig = (HashMap<String, Object>) topologyConf
				.get("input");
		int batchDuration = Integer.parseInt((String) topologyConf
				.get("batching_interval"));
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(batchDuration));

		parseInputs(streams, jssc, inputsConfig);

		ArrayList<Object> filterConfig = (ArrayList<Object>) topologyConf
				.get("filter");

		// kafaf streaming
		JavaDStream<HashMap<String, Object>> trace = (JavaDStream<HashMap<String, Object>>) streams
				.get("mobile");
		// trace.print();

		HashMap<String, Object> splitconf = new HashMap<String, Object>();
		splitconf.put("delimiter", "\\t");
		splitconf.put("src", "message");
		HashMap<String, Integer> fields = new HashMap<String, Integer>();
		fields.put("ServerIP", 1);
		fields.put("ServiceCode", 4);
		fields.put("StartTime", 6);
		fields.put("Interval", 7);
		fields.put("ServiceStatus", 8);

		splitconf.put("fields", fields);
		JavaDStream<HashMap<String, Object>> splited = trace.map(new Split(
				splitconf));
		splited.print();

		HashMap<String, Object> traceDateConf = new HashMap<String, Object>();
		traceDateConf.put("src", "StartTime");
		traceDateConf.put("target", "@timestamp");
		traceDateConf.put("format", "yyyy-MM-dd HH:mm:ss.SSS");
		JavaDStream<HashMap<String, Object>> traceDate = splited.map(new Date(
				traceDateConf));

		traceDate.print();

		/*
		 * JavaPairDStream<ArrayList<String>, HashMap<String, Object>>
		 * traceSetIntervalKey = traceDate .mapToPair(new
		 * PairFunction<HashMap<String, Object>, ArrayList<String>,
		 * HashMap<String, Object>>() {
		 * 
		 * @Override public Tuple2<ArrayList<String>, HashMap<String, Object>>
		 * call( HashMap<String, Object> arg0) throws Exception { // TODO
		 * Auto-generated method stub HashMap<String, Object> event =
		 * (HashMap<String, Object>) arg0; ArrayList<String> tuple_1 = new
		 * ArrayList<String>(); tuple_1.add((String) event.get("ServiceCode"));
		 * int interval = Integer.parseInt((String) event .get("Interval")); if
		 * (interval < 1000) tuple_1.add("low"); else if (interval < 5000)
		 * tuple_1.add("latency"); else tuple_1.add("latencyHigh");
		 * 
		 * if (!event.containsKey("@timestamp")) tuple_1.add("0"); else { long
		 * timestamp = (long) event.get("@timestamp");
		 * tuple_1.add(Long.toString(timestamp - timestamp % 60000)); }
		 * 
		 * return new Tuple2<ArrayList<String>, HashMap<String, Object>>(
		 * tuple_1, event); } });
		 * 
		 * traceSetIntervalKey .reduceByKey( new Function2<HashMap<String,
		 * Object>, HashMap<String, Object>, HashMap<String, Object>>() {
		 * 
		 * @Override public HashMap<String, Object> call( HashMap<String,
		 * Object> a, HashMap<String, Object> b) { HashMap<String, Object>
		 * metric = new HashMap<String, Object>(); long count = 0; if
		 * (a.containsKey("count")) { count = (long) a.get("count"); } else {
		 * count = 1; }
		 * 
		 * if (b.containsKey("count")) { count += (long) b.get("count"); } else
		 * { count += 1; } metric.put("count", count); return metric; }
		 * }).print();
		 */

		// traceRaw.print();
		// traceDate.print();
		// traceSetIntervalKey.print();

		// Map<String, Integer> paymentMap = new HashMap<String, Integer>();
		// paymentMap.put("logstash-logginggw-mobile-paymentinfosoalog", 1);
		//
		// JavaPairReceiverInputDStream<String, String> payment = KafkaUtils
		// .createStream(jssc, zkQuorum, group, paymentMap);
		//
		// // grok trace
		// JavaDStream<HashMap<String, Object>> paymentRaw = payment
		// .map(new Function<Tuple2<String, String>, HashMap<String, Object>>()
		// {
		// @SuppressWarnings("unchecked")
		// @Override
		// public HashMap<String, Object> call(
		// Tuple2<String, String> tuple2) {
		// return (HashMap<String, Object>) JSONValue.parse(tuple2
		// ._2());
		// }
		// });
		//
		// // JavaDStream<HashMap<String, Object>> mutate = raw.map(new
		// Mutate());
		//
		// HashMap<String, Object> dateconf = new HashMap<String, Object>();
		// dateconf.put("src", "StartTime");
		// dateconf.put("format", "yyyy/MM/dd HH:mm:ss");
		// JavaDStream<HashMap<String, Object>> paymentDate = paymentRaw
		// .map(new Date(dateconf));
		//
		// JavaDStream<HashMap<String, Object>> paymentFail = paymentDate
		// .filter(new Function<HashMap<String, Object>, Boolean>() {
		//
		// @Override
		// public Boolean call(HashMap<String, Object> arg0)
		// throws Exception {
		// // TODO Auto-generated method stub
		// HashMap<String, Object> event = (HashMap<String, Object>) arg0;
		// if (event == null) {
		// return false;
		// }
		//
		// return event.containsKey("IsSuccessful")
		// && (Boolean) event.get("IsSuccessful") == true;
		// }
		// });

		jssc.start();
		jssc.awaitTermination();
	}
}
