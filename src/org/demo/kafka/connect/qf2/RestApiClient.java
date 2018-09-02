// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.collections4.map.SingletonMap;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.MultiMap;
import org.eclipse.jetty.util.UrlEncoded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiClient {

	private String bearer = "";

	private static Map<String, String> props = null;

	private static RestApiClient instance = null;

	private static final Logger log = LoggerFactory.getLogger(RestApiClient.class);

	public static final String PASSWORD_CONFIG = "qq.password";

	public static final String USERNAME_CONFIG = "qq.username";

	public static final String HOSTNAME_CONFIG = "qq.hostname";

	public static final String PAGESIZE_CONFIG = "qq.pagesize";

	public static class Entry {

		private String name = null;

		private String path = null;

		private String ctime = null;

		private String type = null;

		private Long size = null;

		public Entry(Map<String, String> attrs) {
			setName(attrs.get("name"));
			setPath(attrs.get("path"));
			setSize(Long.valueOf(attrs.get("size")));
			setCtime(attrs.get("change_time"));
			setType(attrs.get("type"));
		}

		public Entry(String name, String path, Long size, String type, String ctime) {
			setName(name);
			setPath(path);
			setSize(size);
			setCtime(ctime);
			setType(type);
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getPath() {
			return this.path;
		}

		public void setPath(String path) {
			this.path = path;
		}

		public String getCtime() {
			return this.ctime;
		}

		public void setCtime(String ctime) {
			this.ctime = ctime;
		}

		public String getType() {
			return this.type;
		}

		public void setType(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return "Path: " + getPath() + "\tName: " + getName() + "\tCtime: " + getCtime() + "\tSize: " + getSize()
					+ "\tType: " + getType();
		}

		public Long getSize() {
			return this.size;
		}

		public void setSize(Long size) {
			this.size = size;
		}
	}

	protected RestApiClient() throws Exception {

		if (RestApiClient.props == null)
			throw new Exception(
					"Cannot perform RestApiUtil() before completing a call to getInstance(Map<String, String> props)");
		initSSLContext();
		allowMethods("PATCH");
		this.bearer = login();
		if (this.bearer.isEmpty())
			throw new Exception("login() failed!");
	}

	public static synchronized RestApiClient getInstance(Map<String, String> props) throws Exception {

		RestApiClient.props = new HashMap<String, String>(props);
		return getInstance();
	}

	public static synchronized RestApiClient getInstance() throws Exception {

		if (RestApiClient.instance == null)
			RestApiClient.instance = new RestApiClient();

		return RestApiClient.instance;
	}

	public String getBearer() throws Exception {

		if (RestApiClient.instance == null) {
			throw new Exception("qqRestApi is missing its singleton");
		}
		log.trace("getBearer: bearer:{}", this.bearer);
		return this.bearer;
	}

	public void initSSLContext() throws Exception {

		HttpsURLConnection.setDefaultHostnameVerifier(new javax.net.ssl.HostnameVerifier() {
			public boolean verify(String hostname, SSLSession sslSession) {
				String endpoint = null;
				try {
					endpoint = props.get(HOSTNAME_CONFIG);
				} catch (Exception e) {
					endpoint = "localhost";
				}
				return hostname.equals(endpoint);
			}
		});

		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return new java.security.cert.X509Certificate[0];
			}

			public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
			}

			public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
			}
		} };

		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

	}

	public Map<Long, Entry> listFiles(String path, boolean paging) throws Exception {
		Map<Long, Entry> pages = new HashMap<Long, Entry>();
		try {
			list(path, null, true, false, paging, null, pages);
		} catch (Exception e) {
			pages = null;
		}
		return pages;
	}

	public Map<Long, Entry> listDirectories(String path, boolean paging) throws Exception {
		Map<Long, Entry> pages = new HashMap<Long, Entry>();
		try {
			list(path, null, false, true, paging, null, pages);
		} catch (Exception e) {
			pages = null;
		}
		return pages;
	}

	public Long listFiles(String path, Map<Long, Entry> pages) throws Exception {
		return list(path, null, true, false, false, null, pages);
	}

	public Long listDirectories(String path, Map<Long, Entry> pages) throws Exception {
		return list(path, null, false, true, false, null, pages);
	}

	public Long list(String path, boolean fileType, boolean directoryType, Map<Long, Entry> pages) throws Exception {
		return list(path, null, true, false, false, null, pages);
	}

	public Long list(String path, Long snapid, boolean fileType, boolean directoryType, boolean paging, Long after,
			Map<Long, Entry> pages) throws Exception {

		Long count = null;
		String uri = getEncodedURL(path);
		log.trace("list: path:{}\tsnapid:{}\tfileType:{}\tdirectoryType:{}\tpaging:{}\tafter:{}\tpages:{}", path,
				snapid, fileType, directoryType, paging, after, pages.size());

		String str = null;
		if (after != null && after.longValue() > 0) {
			str = new String("https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri
					+ "/entries/?after=" + after.toString() + "&limit=" + RestApiClient.props.get(PAGESIZE_CONFIG));
		} else {
			str = new String("https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri
					+ "/entries/?limit=" + RestApiClient.props.get(PAGESIZE_CONFIG));
		}
		if (snapid != null && snapid.longValue() > 0L)
			str += "&snapshot=" + snapid.toString();
		URL url = new URL(str);
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "*/*");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setUseCaches(false);
		log.trace("list:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();
		JsonReader reader = Json.createReader(new StringReader(buffer));

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (!err.equals("fs_no_such_entry_error")) {
				throw new Exception(buffer);
			}
		} else {
			JsonObject all = (JsonObject) reader.readObject();
			JsonArray entries = all.getJsonArray("files");
			if (!fileType && !directoryType) {
				fileType = true;
				directoryType = false;
			}
			for (JsonValue entry : entries) {
				JsonObject o = entry.asJsonObject();
				String type = o.getString("type");
				if ((fileType && type.equals("FS_FILE_TYPE_FILE"))
						|| (directoryType && type.equals("FS_FILE_TYPE_DIRECTORY")))
					pages.put(Long.valueOf(o.getString("id")), new Entry(o.getString("name"), o.getString("path"),
							Long.valueOf(o.getString("size")), type, o.getString("change_time")));

			}
			if (paging) {
				JsonObject o = all.getJsonObject("paging");
				String next = o.getString("next");
				if (next != null && !next.isEmpty()) {
					url = new URL("https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000" + next);
					MultiMap<String> query = new MultiMap<String>();
					UrlEncoded.decodeTo(url.getQuery(), query, "UTF-8", 2);
					list(path, snapid, fileType, directoryType, true, Long.valueOf(query.getString("after")), pages);
				}
			}
			count = Integer.valueOf(pages.size()).longValue();
		}

		return count;
	}

	public byte[] readFile(Long id) throws Exception {

		byte[] data = null;
		log.trace("readFile: id:{}", id);

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/" + id + "/data")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "*/*");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setUseCaches(false);
		log.trace("readFile:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			throw new Exception(buffer);
		else
			data = buffer.getBytes();

		return data;
	}

	public byte[] readFile(String path, String filename) throws Exception {

		byte[] data = null;
		String uri = getEncodedURL(path + "/" + filename);
		log.trace("readFile: path:{}\tfilename:{}", path, filename);

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "/data")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "*/*");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setUseCaches(false);
		log.trace("readFile:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			throw new Exception(buffer);
		else
			data = buffer.getBytes();

		return data;
	}

	public void delete(long id) throws Exception {
		delete(id, false);
	}

	public void delete(long id, boolean snapshot) throws Exception {

		HttpsURLConnection conn = null;
		log.trace("delete: id:{}\tsnapshot:{}", id, snapshot);
		if (snapshot) {
			conn = (HttpsURLConnection) new URL(
					"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v2/snapshots/" + id)
							.openConnection();
		} else {
			conn = (HttpsURLConnection) new URL(
					"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/" + id).openConnection();
		}
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("DELETE");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", "0");
		conn.setUseCaches(false);
		log.trace("delete:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			is = conn.getErrorStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
			JsonReader reader = Json.createReader(new StringReader(buffer));
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (!err.equals("fs_no_such_entry_error")) {
				throw new Exception(buffer);
			}
		}
	}

	public void delete(String path, String name) throws Exception {

		String uri = getEncodedURL(path);
		log.trace("delete: path:{}\tname:{}", path, name);

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "%2F" + name)
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("DELETE");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", "0");
		conn.setUseCaches(false);
		log.trace("delete:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			is = conn.getErrorStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
			JsonReader reader = Json.createReader(new StringReader(buffer));
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (!err.equals("fs_no_such_entry_error")) {
				throw new Exception(buffer);
			}
		}
	}

	public void deleteTree(String path) throws Exception {

		String uri = getEncodedURL(path);
		log.trace("deleteTree: path:{}", path);

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "/delete-tree")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", "0");
		conn.setUseCaches(false);
		log.trace("deleteTree:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			is = conn.getErrorStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
			JsonReader reader = Json.createReader(new StringReader(buffer));
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (!err.equals("fs_no_such_entry_error")) {
				throw new Exception(buffer);
			}
		}
	}

	public Map<String, String> attributes(String path) throws Exception {

		Map<String, String> result = null;
		String uri = getEncodedURL(path);
		log.trace("attributes: path:{}", path);

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "/info/attributes")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "*/*");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setUseCaches(false);
		log.trace("attributes:" + conn.toString());

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();
		
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			throw new Exception(buffer);
		else {
			result = new HashMap<String, String>();
			JsonReader reader = Json.createReader(new StringReader(buffer));
			JsonObject all = (JsonObject) reader.readObject();
			for (Map.Entry<String, JsonValue> entry : all.entrySet()) {
				String key = StringUtils.removeStart(entry.getKey(), "\"");
				key = StringUtils.removeEnd(key, "\"");
				String value = StringUtils.removeStart(entry.getValue().toString(), "\"");
				value = StringUtils.removeEnd(value, "\"");
				result.put(key, value);
			}
		}

		return result;
	}

	public Long rename(String path, String oldpath, String newname) throws Exception {

		Long id = null;
		String uri = getEncodedURL(path);
		oldpath = normalizePath(oldpath);
		newname = normalizeName(newname);
		log.trace("rename: path:{}\toldpath:{}\tnewname:{}", path, oldpath, newname);

		String qbody = "{\"action\": \"RENAME\",\"old_path\": \"/" + oldpath + "\",\"name\": \"" + newname + "\"}";
		log.trace(qbody);
		byte[] bdata = qbody.getBytes(StandardCharsets.UTF_8);
		int clength = bdata.length;

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "/entries/")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("rename:" + conn.toString());

		DataOutputStream os = new DataOutputStream(conn.getOutputStream());
		os.write(bdata);

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();
		JsonReader reader = Json.createReader(new StringReader(buffer));

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (!err.equals("fs_entry_exists_error") || !err.equals("fs_no_such_entry_error")) {
				throw new Exception(buffer);
			}
		} else {
			JsonObject o = (JsonObject) reader.readObject();
			id = Long.valueOf(o.getString("id"));
		}

		return id;
	}

	private String normalizeName(String name) {

		name = (name == null) ? ("") : (StringUtils.deleteWhitespace(name));
		name = StringUtils.removeAll(name, "(?is)[A-Z]:");
		name = StringUtils.remove(name, ":");
		name = name.replace("\\", "/");
		name = StringUtils.replaceAll(name, "%2F", "/");
		String[] dirs = name.split("/");
		name = dirs[0];
		for (String dir : dirs) {
			dir = StringUtils.removeAll(dir, "[^A-Za-z0-9.\\-\\_]");
			if (!dir.isEmpty() && !dir.equals(".") && !dir.equals("..")) {
				name = dir;
				break;
			}
		}
		return name;
	}

	private String normalizePath(String path) {

		path = (path == null) ? ("") : (StringUtils.deleteWhitespace(path));
		path = StringUtils.removeAll(path, "(?is)[A-Z]:");
		path = StringUtils.remove(path, ":");
		path = path.replace("\\", "/");
		path = StringUtils.replaceAll(path, "%2F", "/");
		String[] dirs = path.split("/");
		path = "";
		for (String dir : dirs) {
			dir = StringUtils.removeAll(dir, "[^A-Za-z0-9.\\-\\_]");
			if (!dir.isEmpty() && !dir.equals(".") && !dir.equals(".."))
				path += "/" + dir;
		}
		path = StringUtils.stripStart(path, "/");
		path = StringUtils.stripEnd(path, "/");
		return path;
	}

	private String[] splitPath(String path) {
		return normalizePath(path).split("/");
	}

	public void createDirectory(String dir) throws Exception {
		createDirectory(splitPath(dir));
	}

	public String getEncodedURL(String path) throws Exception {
		return URLEncoder.encode(normalizePath(path), "UTF-8");
	}

	public void createDirectory(String[] dirs) throws Exception {

		String parent = "";
		for (String dir : dirs) {
			if (parent.isEmpty())
				createDirectory("", dir);
			else
				createDirectory(parent, dir);
			parent += (parent.isEmpty()) ? (dir) : ("/" + dir);
		}
	}

	public void createDirectory(String path, String name) throws Exception {

		String uri = getEncodedURL(path);
		name = normalizeName(name);
		log.trace("attributes: path:{}\tname:{}", path, name);

		String qbody = "{\"action\": \"CREATE_DIRECTORY\",\"name\": \"" + name + "\"}";
		log.trace(qbody);
		byte[] bdata = qbody.getBytes(StandardCharsets.UTF_8);
		int clength = bdata.length;

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "/entries/")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("createDirectory:" + conn.toString());

		DataOutputStream os = new DataOutputStream(conn.getOutputStream());
		os.write(bdata);

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		String err = "";
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			is = conn.getErrorStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
			JsonReader reader = Json.createReader(new StringReader(buffer));
			JsonObject o = (JsonObject) reader.readObject();
			err = o.getString("error_class");
			if (!err.equals("fs_entry_exists_error")) {
				throw new Exception(buffer);
			}
		}

	}

	public SingletonMap<Long, String> createSnapshot(String path, String name) throws Exception {

		SingletonMap<Long, String> result = null;
		name = normalizeName(name);
		log.trace("createSnapshot: path:{}\tname:{}", path, name);

		Map<String, String> attrs = attributes(path);
		String body = "{\"name\": \"" + name + "\",\"source_file_id\": \"" + attrs.get("id") + "\"}";
		log.trace(body);
		byte[] bdata = body.getBytes(StandardCharsets.UTF_8);
		int clength = bdata.length;

		HttpsURLConnection conn = (HttpsURLConnection) new URL("https://" + RestApiClient.props.get(HOSTNAME_CONFIG)
				+ ":8000/v2/snapshots/?expiration-time-to-live=1hours").openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("createSnapshot:" + conn.toString());

		DataOutputStream os = new DataOutputStream(conn.getOutputStream());
		os.write(bdata);

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			throw new Exception(buffer);
		else {
			JsonReader reader = Json.createReader(new StringReader(buffer));
			JsonObject o = (JsonObject) reader.readObject();
			result = new SingletonMap<Long, String>(Integer.valueOf(o.getInt("id")).longValue(),
					o.getString("directory_name"));
		}

		return result;
	}

	public Long writeFile(String path, String filename, byte[] data) throws Exception {
		return writeFile(path, filename, data, false);
	}

	public Long writeFile(String path, String filename, byte[] data, boolean createPath) throws Exception {

		Long id = null;
		if (createPath)
			createDirectory(path);
		String uri = getEncodedURL(path);
		filename = normalizeName(filename);
		log.trace("readFile: path:{}\tfilename:{}\tdata:{}\tcreatePath:{}", path, filename, data.length, createPath);

		String qbody = "{\"action\": \"CREATE_FILE\",\"name\": \"" + filename + "\"}";
		log.trace(qbody);
		byte[] bdata = qbody.getBytes(StandardCharsets.UTF_8);
		int clength = bdata.length;

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F" + uri + "/entries/")
						.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("writeFile:" + conn.toString());

		DataOutputStream os = new DataOutputStream(conn.getOutputStream());
		os.write(bdata);

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();
		JsonReader reader = Json.createReader(new StringReader(buffer));

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (err.equals("fs_entry_exists_error")) {
				Map<String, String> attrs = attributes(path + "/" + filename);
				id = Long.valueOf(attrs.get("id"));
			} else
				throw new Exception(buffer);
		} else {
			JsonObject o = (JsonObject) reader.readObject();
			id = Long.valueOf(o.getString("id"));
		}

		clength = data.length;

		conn = (HttpsURLConnection) new URL("https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/files/%2F"
				+ uri + "%2F" + filename + "/data").openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("PUT");
		conn.setRequestProperty("Content-Type", "application/octet-stream; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("writeFile:" + conn.toString());

		os = new DataOutputStream(conn.getOutputStream());
		os.write(data);

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			is = conn.getErrorStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
			br.close();
			reader = Json.createReader(new StringReader(buffer));
			JsonObject o = (JsonObject) reader.readObject();
			String err = o.getString("error_class");
			if (!err.equals("fs_entry_exists_error")) {
				id = null;
				throw new Exception(buffer);
			}
		}
		return id;
	}

	private void allowMethods(String... methods) {
		try {

			Field methodsField = HttpURLConnection.class.getDeclaredField("methods");

			Field modifiersField = Field.class.getDeclaredField("modifiers");
			modifiersField.setAccessible(true);
			modifiersField.setInt(methodsField, methodsField.getModifiers() & ~Modifier.FINAL);

			methodsField.setAccessible(true);

			String[] oldMethods = (String[]) methodsField.get(null);
			Set<String> methodsSet = new LinkedHashSet<>(Arrays.asList(oldMethods));
			methodsSet.addAll(Arrays.asList(methods));
			String[] newMethods = methodsSet.toArray(new String[0]);

			methodsField.set(null, newMethods);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
	}

	public void patchFile(String path, byte[] data) throws Exception {

		String uri = getEncodedURL(path);
		Map<String, String> attrs = attributes(path);
		log.trace("patchFile: path:{}\tdata:{}", path, data.length);
		if (data.length == 0)
			throw new IllegalArgumentException("Empty data");

		int clength = data.length;
		HttpsURLConnection conn = (HttpsURLConnection) new URL("https://" + RestApiClient.props.get(HOSTNAME_CONFIG)
				+ ":8000/v1/files/%2F" + uri + "/data?offset=" + attrs.get("size")).openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("PATCH");
		conn.setRequestProperty("Content-Type", "application/octet-stream; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Authorization", "Bearer " + this.bearer);
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("patchFile:" + conn.toString());

		DataOutputStream os = new DataOutputStream(conn.getOutputStream());
		os.write(data);

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500) {
			is = conn.getErrorStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
			throw new Exception(buffer);
		}
	}

	private String login() throws Exception {

		String pauth = "{\"username\": \"" + RestApiClient.props.get(USERNAME_CONFIG) + "\",\"password\": \""
				+ RestApiClient.props.get(PASSWORD_CONFIG) + "\"}";
		byte[] bdata = pauth.getBytes(StandardCharsets.UTF_8);
		int clength = bdata.length;
		log.trace("login: username:{}", RestApiClient.props.get(USERNAME_CONFIG));

		HttpsURLConnection conn = (HttpsURLConnection) new URL(
				"https://" + RestApiClient.props.get(HOSTNAME_CONFIG) + ":8000/v1/session/login").openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("content", "{}");
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		conn.setRequestProperty("charset", "utf-8");
		conn.setRequestProperty("Content-Length", Integer.toString(clength));
		conn.setUseCaches(false);
		log.trace("login:" + conn.toString());

		DataOutputStream os = new DataOutputStream(conn.getOutputStream());
		os.write(bdata);

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		if (400 <= conn.getResponseCode() && conn.getResponseCode() < 500)
			is = conn.getErrorStream();
		else
			is = conn.getInputStream();
		isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		String buffer = br.lines().collect(Collectors.joining(System.lineSeparator()));
		br.close();

		String bearer = "";
		if (200 <= conn.getResponseCode() && conn.getResponseCode() < 300) {
			try {
				JsonReader reader = Json.createReader(new StringReader(buffer));
				JsonObject o = (JsonObject) reader.readObject();
				bearer = o.getString("bearer_token");
			} catch (Exception e) {
				bearer = "";
			}
		} else
			throw new Exception(buffer);

		return bearer;
	}
}
