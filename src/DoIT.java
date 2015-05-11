import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.tree.Node;

import scala.Tuple2;
import transformation.Date;
import transformation.Split;
import utils.firstProcess.Json;
import utils.firstProcess.Plain;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

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
					System.out.println("1" + streamID);

					// kafaf streaming
					HashMap<String, Object> oneInputConfigWithCertainType = (HashMap<String, Object>) oneInputWithCertainTypeEntry
							.getValue();
					HashMap<String, String> topicsMap = (HashMap<String, String>) oneInputConfigWithCertainType
							.get("topics");
					HashMap<String, Integer> topics = new HashMap<String, Integer>();
					Iterator<Entry<String, String>> topicIT = topicsMap
							.entrySet().iterator();
					while (topicIT.hasNext()) {
						Map.Entry<String, String> entry = topicIT.next();
						topics.put(entry.getKey(),
								Integer.parseInt(entry.getValue()));
					}

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
							.createStream(jssc, zkQuorum, group, topics);

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

		YamlReader reader = null;
		HashMap<String, Object> topologyConf = null;
		try {
			reader = new YamlReader(new FileReader(args[0]));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		try {
			topologyConf = (HashMap<String, Object>) reader.read();
		} catch (YamlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
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
		System.out.println(streams);

		// kafaf streaming
		JavaDStream<HashMap<String, Object>> trace = (JavaDStream<HashMap<String, Object>>) streams
				.get("mobile");


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

		HashMap<String, Object> traceDateConf = new HashMap<String, Object>();
		traceDateConf.put("src", "StartTime");
		traceDateConf.put("target", "@timestamp");
		traceDateConf.put("format", "yyyy-MM-dd HH:mm:ss.SSS");
		JavaDStream<HashMap<String, Object>> traceDate = splited.map(new Date(
				traceDateConf));

		JavaPairDStream<ArrayList<String>, HashMap<String, Object>> traceSetIntervalKey = traceDate
				.mapToPair(new PairFunction<HashMap<String, Object>, ArrayList<String>, HashMap<String, Object>>() {

					@Override
					public Tuple2<ArrayList<String>, HashMap<String, Object>> call(
							HashMap<String, Object> arg0) throws Exception {
						// TODO Auto-generated method stub
						HashMap<String, Object> event = (HashMap<String, Object>) arg0;
						ArrayList<String> tuple_1 = new ArrayList<String>();
						tuple_1.add((String) event.get("ServiceCode"));
						int interval = Integer.parseInt((String) event
								.get("Interval"));
						if (interval < 1000)
							tuple_1.add("low");
						else if (interval < 5000)
							tuple_1.add("latency");
						else
							tuple_1.add("latencyHigh");

						if (!event.containsKey("@timestamp"))
							tuple_1.add("0");
						else {
							long timestamp = (long) event.get("@timestamp");
							tuple_1.add(Long.toString(timestamp - timestamp
									% 60000));
						}

						return new Tuple2<ArrayList<String>, HashMap<String, Object>>(
								tuple_1, event);
					}
				});

		traceSetIntervalKey
				.reduceByKey(
						new Function2<HashMap<String, Object>, HashMap<String, Object>, HashMap<String, Object>>() {
							@Override
							public HashMap<String, Object> call(
									HashMap<String, Object> a,
									HashMap<String, Object> b) {
								HashMap<String, Object> metric = new HashMap<String, Object>();
								long count = 0;
								if (a.containsKey("count")) {
									count = (long) a.get("count");
								} else {
									count = 1;
								}

								if (b.containsKey("count")) {
									count += (long) b.get("count");
								} else {
									count += 1;
								}
								metric.put("count", count);
								return metric;
							}
						}).print();

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
