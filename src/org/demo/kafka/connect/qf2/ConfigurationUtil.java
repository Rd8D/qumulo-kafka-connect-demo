// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class ConfigurationUtil {

	public static <K, V> Map<K, V> propsToMap(Properties props) {

		Map<K, V> result = new HashMap<>();
		for (final Entry<Object, Object> entry : props.entrySet()) {
			final Map.Entry<?, ?> entry2 = entry;
			final Object key = entry2.getKey();
			final Object value = entry2.getValue();
			result.put((K) key, (V) value);
		}
		return result;
	}

	public static Properties loadProps(String propsFile) throws IOException {

		Properties props = new Properties();
		if (propsFile != null) {
			try (InputStream propStream = new FileInputStream(propsFile)) {
				props.load(propStream);
			}
		}
		return props;
	}

	public static <K, V> Properties mapToprops(final Map<K, V> map) {
		final Properties props = new Properties();
		if (map != null) {
			for (final Entry<K, V> entry2 : map.entrySet()) {
				final Map.Entry<?, ?> entry = entry2;
				final Object key = entry.getKey();
				final Object value = entry.getValue();
				props.put(key, value);
			}
		}
		return props;
	}
}
