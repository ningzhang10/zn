package com.tenddata.collector.util;

import org.codehaus.jackson.map.ObjectMapper;

public class JacksonMapper {

	private static final ObjectMapper mapper = new ObjectMapper();

	private JacksonMapper() {
	}

	public static ObjectMapper getObjectMapper() {
		return mapper;
	}
}