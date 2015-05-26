import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.lang.reflect.Constructor;

import utils.firstProcess.Json;
import utils.firstProcess.Plain;
import utils.firstProcess.RemoveNull;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
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
						streams.put(streamID, a.filter(new RemoveNull())
								.mapToPair(new Json()));
					} else if (codec.equalsIgnoreCase("plain")) {
						streams.put(streamID, a.filter(new RemoveNull())
								.mapToPair(new Plain()));
					}

				}
			}
		}
	}

	private static void buildFunction(HashMap<String, Object> streams,
			ArrayList<Object> filterConfig) throws Exception {
		System.out.println(filterConfig);

		if (filterConfig == null) {
			return;
		}

		for (Object object : filterConfig) {

			System.out.println(object);

			HashMap<String, Object> _config = (HashMap<String, Object>) object;
			Entry _conf = _config.entrySet().iterator().next();
			String filterType = (String) _conf.getKey();
			HashMap<String, Object> config = (HashMap<String, Object>) _conf
					.getValue();

			// from stream

			String streamId = (String) config.get("id");
			String from = (String) config.get("from");

			Object fromStream = streams.get(from);

			// transforation mothod

			String _transformation = null;
			Method transformation = null;
			Object newStream = null;
			Class c = null;
			try { // try if it is our function.Function such as grok/date/mutate
				c = Class.forName("function." + filterType);
			} catch (ClassNotFoundException e) {
				// it's not Function, it is transformation such as window
			}

			if (c != null) {
				if (config.containsKey("transformation")) {
					_transformation = (String) config.get("transformation");
				} else {
					_transformation = (String) c.getField(
							"defaultTransformation").get(null);
				}

				transformation = fromStream.getClass().getMethod(
						_transformation, c.getInterfaces()[0]);

				Constructor cc = c.getConstructor(HashMap.class);
				newStream = transformation.invoke(fromStream,
						cc.newInstance(config));

			} else {
				_transformation = filterType;

				// really difficult for me to deal with java&java Api.
				// for example, two integer are passed to window function, but
				// they are needed to converted to Duration type.
				// I could format the config like [(60,Duration),(10,Duration)],
				// but any other constructor method?

				if (_transformation.equalsIgnoreCase("window")) {
					transformation = fromStream.getClass().getMethod(
							_transformation, Duration.class, Duration.class);
					ArrayList<Integer> transform_args = (ArrayList<Integer>) config
							.get("transform_args");
					newStream = transformation.invoke(fromStream, new Duration(
							transform_args.get(0) * 1000), new Duration(
							transform_args.get(1) * 1000));
				} else if (_transformation.equalsIgnoreCase("union")) {

					ArrayList<String> right = (ArrayList<String>) config
							.get("right");
					transformation = fromStream.getClass().getMethod(
							_transformation,
							streams.get(right.get(0)).getClass());
					Object nextStream = fromStream;
					for (String next : right) {
						nextStream = transformation.invoke(nextStream,
								streams.get(next));
					}
					newStream = nextStream;
				} else {
					Class[] p = {};
					ArrayList<Class> parameters = new ArrayList<Class>();
					ArrayList transform_args = (ArrayList) config
							.get("transform_args");
					if (transform_args != null) {
						for (Object arg : (ArrayList) config
								.get("transform_args")) {
							parameters.add(arg.getClass());
						}
						p = parameters.toArray(p);
					}

					transformation = fromStream.getClass().getMethod(
							_transformation, p);

					newStream = transformation.invoke(fromStream,
							transform_args);
				}
			}

			streams.put(streamId, newStream);
		}

	}

	private static void buildOutput(HashMap<String, Object> streams,
			ArrayList<Object> outputConfig) throws Exception {

		for (Object object : outputConfig) {
			System.out.println(object);

			HashMap<String, Object> _config = (HashMap<String, Object>) object;
			Entry _conf = _config.entrySet().iterator().next();

			String filterType = (String) _conf.getKey();
			HashMap<String, Object> config = (HashMap<String, Object>) _conf
					.getValue();

			String from = (String) config.get("from");

			Object fromStream = streams.get(from);

			String _transformation = null;
			Method transformation = null;
			Object newStream = null;
			Class c = null;
			try { // try if it is our function.Function such as grok/date/mutate
				c = Class.forName("output." + filterType);
			} catch (ClassNotFoundException e) {
				// it's not Function, it is transformation such as window
			}

			if (c != null) {
				if (config.containsKey("transformation")) {
					_transformation = (String) config.get("transformation");
				} else {
					_transformation = (String) c.getField(
							"defaultTransformation").get(null);
				}

				transformation = fromStream.getClass().getMethod(
						_transformation, c.getInterfaces()[0]);

				Constructor cc = c.getConstructor(HashMap.class);
				newStream = transformation.invoke(fromStream,
						cc.newInstance(config));

			} else {
				_transformation = filterType;

				Class[] p = {};
				ArrayList<Class> parameters = new ArrayList<Class>();

				ArrayList transform_args = (ArrayList) config
						.get("transform_args");
				if (transform_args != null) {
					for (Object arg : (ArrayList) config.get("transform_args")) {
						parameters.add(arg.getClass());
					}
					p = parameters.toArray(p);
					transformation = fromStream.getClass().getMethod(
							_transformation, p);

					transformation.invoke(fromStream, transform_args);
				}

				transformation = fromStream.getClass().getMethod(
						_transformation, p);
				transformation.invoke(fromStream);
			}
		}
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
		String master = (String) topologyConf.get("master");
		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(
				master);

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
