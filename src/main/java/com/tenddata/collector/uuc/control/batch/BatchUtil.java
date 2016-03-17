package com.tenddata.collector.uuc.control.batch;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.tenddata.collector.util.Configuration;

public class BatchUtil {

	public static String getBatchFileNamePrefix() {
		return new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
	}
	
	public static String getHDFSPath(String fileName) {
		String path = "/tdinnertest/collector/";
		String events = Configuration.get("batch.logger.t1.events");
		for (String event : events.split(",")) {
			if (fileName.contains(event)) {
				path += event + "/" + fileName;
			}
		}
		return path;
	}
}
