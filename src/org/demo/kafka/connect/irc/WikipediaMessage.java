/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.demo.kafka.connect.irc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

//{,"added":31,"":"Botskapande Indonesien omdirigering","":35,"isNew":true,"isMinor":false,"delta":31,"isAnonymous":false,"user":"Lsjbot","deltaBucket":0.0,"deleted":0,"namespace":"Main"}

public class WikipediaMessage extends Struct {

	final public static Schema SCHEMA = SchemaBuilder.struct().name("WikipediaMessage").version(1)
			.field("isRobot", Schema.BOOLEAN_SCHEMA).field("channel", Schema.STRING_SCHEMA)
			.field("timestamp", Schema.STRING_SCHEMA).field("flags", Schema.STRING_SCHEMA)
			.field("isUnpatrolled", Schema.BOOLEAN_SCHEMA).field("page", Schema.STRING_SCHEMA)
			.field("diffUrl", Schema.STRING_SCHEMA).field("added", Schema.INT32_SCHEMA)
			.field("comment", Schema.STRING_SCHEMA).field("commentLength", Schema.INT32_SCHEMA)
			.field("isNew", Schema.BOOLEAN_SCHEMA).field("isMinor", Schema.BOOLEAN_SCHEMA)
			.field("delta", Schema.INT32_SCHEMA).field("isAnonymous", Schema.BOOLEAN_SCHEMA)
			.field("user", Schema.STRING_SCHEMA).field("deltaBucket", Schema.FLOAT32_SCHEMA)
			.field("deleted", Schema.INT32_SCHEMA).field("namespace", Schema.STRING_SCHEMA).build();

	public WikipediaMessage() {
		super(SCHEMA);
	}

	@Override
	public String toString() {
		return "channel: " + this.get("channel") + "\tuser: " + this.get("user") + "\ttimestamp: "
				+ this.get("timestamp") + "\tadded: " + this.get("added") + "\tcomment: " + this.get("comment")
				+ "\tcommentLength: " + this.get("commentLength") + "\tdeltaBucket: " + this.get("deltaBucket")
				+ "\tdeleted: " + this.get("deleted") + "\tnamespace: " + this.get("namespace") + "\tpage: "
				+ this.get("page") + "\tflags: " + this.get("flags") + "\tdiffUrl: " + this.get("diffUrl") + "\tuser: "
				+ this.get("user") + "\tdelta: " + this.get("delta") + "\tcomment: " + this.get("comment") + "\tisNew: "
				+ this.get("isNew") + "\tisMinor: " + this.get("isMinor") + "\tisUnpatrolled: "
				+ this.get("isUnpatrolled") + "\tisRobot: " + this.get("isRobot") + "\tisAnonymous: "
				+ this.get("isAnonymous");
	}

	public static boolean filterNonNull(String key, WikipediaMessage value) {
		return key != null && value != null;
	}

	public static WikipediaMessage parseMessage(String channel, String raw) {
		WikipediaMessage message = new WikipediaMessage();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		message.put("timestamp", sdf.format(new Date()));
		message.put("channel", channel);
		message.parseText(raw);
		return message;
	}

	public void parseText(String raw) {
		Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
		Matcher m = p.matcher(raw);

		if (!m.find()) {
			throw new IllegalArgumentException("Could not parse message: " + raw);
		} else if (m.groupCount() != 6) {
			throw new IllegalArgumentException("Unexpected parser group count: " + m.groupCount());
		} else {
			int delta = Integer.parseInt(m.group(5));
			this.put("added", (delta >= 0) ? (delta) : (0));
			this.put("deltaBucket", Float
					.valueOf(new Double((delta <= 100) ? (100.0) : (Math.ceil(delta / 100.0) * 100.0)).toString()));
			this.put("deleted", (delta < 0) ? (delta * -1) : (0));
			this.put("namespace", "main");
			this.put("page", m.group(1));
			this.put("flags", m.group(2));
			this.put("diffUrl", m.group(3));
			this.put("user", m.group(4));
			this.put("delta", delta);
			this.put("comment", m.group(6));
			this.put("commentLength", m.group(6).length());
			this.put("isNew", m.group(2).contains("N"));
			this.put("isMinor", m.group(2).contains("M"));
			this.put("isUnpatrolled", m.group(2).contains("!"));
			this.put("isRobot", m.group(2).contains("B"));
			this.put("isAnonymous", false);
		}
	}

}
