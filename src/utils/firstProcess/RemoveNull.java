package utils.firstProcess;

import org.apache.spark.api.java.function.Function;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;
import jinmanager.JinManager;

public class RemoveNull implements Function {

	public RemoveNull() {
	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		Tuple2 event = (Tuple2) arg0;
		return event._2 != null;
	}

}
