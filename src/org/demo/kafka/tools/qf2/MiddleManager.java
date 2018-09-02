// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.tools.qf2;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.demo.kafka.connect.qf2.RestApiClient;
import org.demo.kafka.utils.qf2.crypto.RSAUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class MiddleManager {

	private static final Logger log = LoggerFactory.getLogger(MiddleManager.class);

	public static final String DIR_CONFIG = "dir";

	public static final String SERVER_CONFIG = "bootstrap.server";

	private static final String SEP = "/";

	private static Map<String, String> props = null;

	private String key;

	private String dir;

	private String server;

	private String hostname;

	private String username;

	private String pagesize;

	private String password;

	private RestApiClient restApiClient = null;

	private RSAUtils rsaUtil;

	private static MiddleManager _instance = null;

	public static synchronized MiddleManager getInstance() throws Exception {

		if (_instance == null)
			_instance = new MiddleManager();

		return _instance;
	}

	public static synchronized MiddleManager getInstance(Map<String, String> props) throws Exception {

		MiddleManager.props = new HashMap<String, String>(props);
		return getInstance();
	}

	protected MiddleManager() throws Exception {

		if (props == null)
			throw new Exception(
					"Cannot perform MiddleManager() before completing a call to getInstance(Map<String, String> props)");

		this.key = props.get(RSAUtils.KEY_CONFIG);
		this.dir = props.get(DIR_CONFIG);
		this.server = props.get(SERVER_CONFIG);
		this.hostname = props.get(RestApiClient.HOSTNAME_CONFIG);
		this.username = props.get(RestApiClient.USERNAME_CONFIG);
		this.password = props.get(RestApiClient.PASSWORD_CONFIG);
		this.pagesize = props.get(RestApiClient.PAGESIZE_CONFIG);
		if (this.key == null || this.key.isEmpty())
			throw new ConnectException(
					"MiddleManager configuration must include '" + RSAUtils.KEY_CONFIG + "' setting");
		if (this.dir == null || this.dir.isEmpty())
			throw new ConnectException("MiddleManager configuration must include '" + DIR_CONFIG + "' setting");
		StringUtils.stripEnd(this.dir, SEP);
		if (!this.dir.startsWith(SEP))
			this.dir = SEP + this.dir;
		if (this.server == null || this.server.isEmpty())
			throw new ConnectException("MiddleManager configuration must include '" + SERVER_CONFIG + "' setting");
		if (this.hostname == null || this.hostname.isEmpty())
			throw new ConnectException(
					"MiddleManager configuration must include '" + RestApiClient.HOSTNAME_CONFIG + "' setting");
		if (this.username == null || this.username.isEmpty())
			throw new ConnectException(
					"MiddleManager configuration must include '" + RestApiClient.USERNAME_CONFIG + "' setting");
		if (this.password == null || this.password.isEmpty())
			throw new ConnectException(
					"MiddleManager configuration must include '" + RestApiClient.PASSWORD_CONFIG + "' setting");
		if (this.pagesize == null || this.pagesize.isEmpty())
			throw new ConnectException(
					"MiddleManager configuration must include '" + RestApiClient.PAGESIZE_CONFIG + "' setting");
		this.restApiClient = RestApiClient.getInstance(props);
		this.rsaUtil = RSAUtils.getInstance(props);
	}

	private static void handleRequest(HttpExchange httpExchange) throws Exception {

		log.trace(httpExchange.getRequestURI().toASCIIString());
		for (Entry<String, List<String>> entry : httpExchange.getRequestHeaders().entrySet()) {
			log.trace(entry.getKey() + "" + entry.getValue().toString());
		}
		int count = 0;
		httpExchange.sendResponseHeaders(200, 0);
		OutputStream os = httpExchange.getResponseBody();
		httpExchange.getResponseHeaders().put("Context-Type",
				Collections.singletonList("Content-Type: application/json; charset=UTF-8"));
		MiddleManager middleManager = MiddleManager.getInstance();
		Map<Long, RestApiClient.Entry> entries = middleManager.restApiClient.listFiles(middleManager.dir, true);
		for (Entry<Long, RestApiClient.Entry> entry : entries.entrySet()) {
			log.trace("handleRequest: file:{}", entry.getValue().toString());
			BufferedReader br = new BufferedReader(
					new StringReader(new String(middleManager.restApiClient.readFile(entry.getKey().longValue()))));
			String digest;
			while ((digest = br.readLine()) != null) {
				try {
					String message = middleManager.rsaUtil.decrypt(digest);
					// log.trace(message);
					JsonReader reader = Json.createReader(new StringReader(message));
					JsonObject json = (JsonObject) reader.readObject();

					for (Map.Entry<String, JsonValue> pair : json.entrySet()) {
						if (pair.getKey().equals("payload")) {
							os.write((pair.getValue().toString() + "\n").getBytes("UTF-8"));
							count += 1;
						}
					}
				} catch (Exception e) {
					if (StringUtils.containsIgnoreCase(ExceptionUtils.getRootCauseMessage(e), "Broken pipe")) {
						os.close();
						throw e;	
					} else
						log.error("Malformed digest : {}", digest);
				}
			}
			log.trace("handleRequest: count: {} line(s)", count);
		}
		os.close();
	}

	public static InetSocketAddress parseAndValidateAddresses(String url) {
		InetSocketAddress addr = null;
		if (url != null && !url.isEmpty()) {
			try {
				String host = getHost(url);
				Integer port = getPort(url);
				if (host == null || port == null)
					throw new ConfigException("Invalid url in " + SERVER_CONFIG + ": " + url);

				addr = new InetSocketAddress(host, port);
				if (addr.isUnresolved()) {
					log.warn("Removing server {} from {} as DNS resolution failed for {}", url, SERVER_CONFIG, host);
					addr = null;
				}
			} catch (IllegalArgumentException e) {
				throw new ConfigException("Invalid port in " + SERVER_CONFIG + ": " + url);
			}
		}
		if (addr == null)
			throw new ConfigException("No resolvable bootstrap url given in " + SERVER_CONFIG);
		return addr;
	}

	public static void main(String[] args) throws Exception {

		Map<String, String> props = Utils.propsToStringMap(Utils.loadProps(args[0]));
		MiddleManager middleManager = MiddleManager.getInstance(props);
		HttpServer httpServer = HttpServer.create(parseAndValidateAddresses(middleManager.server), 0);
		HttpContext httpContext = httpServer.createContext("/");
		httpContext.setHandler(arg0 -> {
			try {
				handleRequest(arg0);
			} catch (Throwable t) {
				StringWriter sw = new StringWriter();
				t.printStackTrace(new PrintWriter(sw));
				log.debug(sw.toString());
			}
		});
		httpServer.start();

	}

}
