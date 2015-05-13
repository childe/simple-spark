import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.lang.reflect.Constructor;

import utils.firstProcess.Json;
import utils.firstProcess.Plain;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.function.Function;
import org.yaml.snakeyaml.Yaml;

public class DoIT {
	@SuppressWarnings({ "unchecked" })
	private static void buildInputs(HashMap<String, Object> streams,
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

	private static void buildFunction(HashMap<String, Object> streams,
			ArrayList<Object> filterConfig) throws Exception {

		for (Object object : filterConfig) {
			HashMap<String, Object> _config = (HashMap<String, Object>) object;
			Entry _conf = _config.entrySet().iterator().next();
			String filterType = (String) _conf.getKey();
			HashMap<String, Object> config = (HashMap<String, Object>) _conf
					.getValue();

			String streamId = (String) config.get("id");
			String from = (String) config.get("from");
			String _transformation = (String) config.get("transformation");

			Method transformation = null;

			Object fromStream = streams.get(from);

			transformation = fromStream.getClass().getMethod(_transformation,
					Function.class);
			Class c = Class.forName("function." + filterType);
			Constructor cc = c.getConstructor(HashMap.class);
			streams.put(streamId,
					transformation.invoke(fromStream, cc.newInstance(config)));

		}

	}

	private static void buildOutput(HashMap<String, Object> streams,
			ArrayList<Object> outputConfig) {

		JavaDStream a = (JavaDStream) streams.get("date");
		a.print();
		/*
		 * for (Object object : outputConfig) { HashMap<String, Object> _config
		 * = (HashMap<String, Object>) object; Entry _conf =
		 * _config.entrySet().iterator().next(); String outputType = (String)
		 * _conf.getKey(); HashMap<String, Object> config = (HashMap<String,
		 * Object>) _conf .getValue();
		 * 
		 * String from = (String) config.get("from"); String _transformation =
		 * (String) config.get("transformation");
		 * 
		 * Method transformation = null;
		 * 
		 * JavaDStream fromStream = (JavaDStream) streams.get(from);
		 * 
		 * transformation = fromStream.getClass().getMethod(_transformation,);
		 * Class c = Class.forName("output." + outputType); Constructor cc =
		 * c.getConstructor(HashMap.class); }
		 */

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
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

		// build streams, it a topology: input -> filter -> output

		HashMap<String, Object> streams = new HashMap<String, Object>();

		// input

		HashMap<String, Object> inputsConfig = (HashMap<String, Object>) topologyConf
				.get("input");
		int batchDuration = (int) topologyConf.get("batching_interval");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(batchDuration));

		buildInputs(streams, jssc, inputsConfig);

		// function

		ArrayList<Object> filterConfig = (ArrayList<Object>) topologyConf
				.get("function");
		buildFunction(streams, filterConfig);

		// output

		ArrayList<Object> outputConfig = (ArrayList<Object>) topologyConf
				.get("output");
		buildOutput(streams, outputConfig);

		jssc.start();
		jssc.awaitTermination();
	}
}
