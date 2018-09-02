// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

public class RestApiSinkConnector extends SinkConnector {

	public static final String DIR_CONFIG = "dir";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(DIR_CONFIG, Type.STRING, Importance.HIGH, "Destination directory")
			.define(RestApiClient.HOSTNAME_CONFIG, Type.STRING, Importance.HIGH, "QF2 hostname")
			.define(RestApiClient.USERNAME_CONFIG, Type.STRING, Importance.HIGH, "QF2 username")
			.define(RestApiClient.PASSWORD_CONFIG, Type.STRING, Importance.HIGH, "QF2 password")
			.define(RestApiClient.PAGESIZE_CONFIG, Type.INT, Importance.HIGH, "QF2 pagesize");

	private String dir;

	private String hostname;

	private String username;

	private String password;

	private String pagesize;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.dir = props.get(DIR_CONFIG);
		this.hostname = props.get(RestApiClient.HOSTNAME_CONFIG);
		this.username = props.get(RestApiClient.USERNAME_CONFIG);
		this.password = props.get(RestApiClient.PASSWORD_CONFIG);
		this.pagesize = props.get(RestApiClient.PAGESIZE_CONFIG);

		if (this.hostname == null || this.hostname.isEmpty())
			throw new ConnectException("RestApiSourceConnector configuration must include '"
					+ RestApiClient.HOSTNAME_CONFIG + "' setting");
		if (this.username == null || this.username.isEmpty())
			throw new ConnectException("RestApiSourceConnector configuration must include '"
					+ RestApiClient.USERNAME_CONFIG + "' setting");
		if (this.password == null || this.password.isEmpty())
			throw new ConnectException("RestApiSourceConnector configuration must include '"
					+ RestApiClient.PASSWORD_CONFIG + "' setting");
		if (this.pagesize == null || this.pagesize.isEmpty())
			throw new ConnectException("RestApiSourceConnector configuration must include '"
					+ RestApiClient.PAGESIZE_CONFIG + "' setting");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return RestApiSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> config = new HashMap<>();
			if (this.dir != null)
				config.put(DIR_CONFIG, this.dir);
			if (this.hostname != null)
				config.put(RestApiClient.HOSTNAME_CONFIG, this.hostname);
			if (this.username != null)
				config.put(RestApiClient.USERNAME_CONFIG, this.username);
			if (this.password != null)
				config.put(RestApiClient.PASSWORD_CONFIG, this.password);
			if (this.pagesize != null)
				config.put(RestApiClient.PAGESIZE_CONFIG, this.pagesize);
			configs.add(config);
		}
		return configs;
	}

	@Override
	public void stop() {
		// Nothing to do here
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}
