// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.commons.qf2;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseStackTrace;
import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

public class ExceptionUtils {

	private static final Logger log = LoggerFactory.getLogger(ExceptionUtils.class);

	public static void log(Logger log, Throwable t) {
		Throwable root = getRootCause(t);
		if (root != null) {
			log.error(getRootCauseMessage(t));
			log.error(StringUtils.join(getRootCauseStackTrace(t)));
		}
		log.error(getMessage(t));
		log.error(getStackTrace(t));
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Exception e = new Exception("chicken");
			throw new Exception("egg", e);
			
		} catch (Exception e) {
			ExceptionUtils.log(log, e);
		}
	}

}
