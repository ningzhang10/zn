package com.tenddata.collector.util;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

/**
 * Datetime ：2012-8-1 下午6:17:19<br>
 * Title : Configuration.java<br>
 * Description: 读取配置<br>
 * Company : Tend<br>
 * 
 * @author <a href="mailto:fanjinhu@gmail.com">fjh</a>
 */
public class Configuration {

	private static Properties properties = new Properties();

	private Configuration() {
	}

	static {
		try {
			properties.load(Thread.currentThread().getContextClassLoader().getResource("config.properties").openStream());
			replace(properties, getConfig());
			System.out.println("配置如下");
			for (Enumeration e = properties.propertyNames(); e.hasMoreElements();) {
				String key = (String) e.nextElement();
				System.out.println(key + "=:" + properties.getProperty(key));
			}
		} catch (final Exception e) {
			System.out.println(e);
		}
	}

	public static Map<String, String> getConfig() {
		try {
			com.tendcloud.config.api.Configuration client = com.tendcloud.config.api.Configuration.getInstance();
			Map<String, String> confs = client.match(".*");
			return confs;
		} catch (Error e) {
			// logger.error("获取数据源加载配置失败", e);
			System.err.println("获取数据源加载配置失败,信息:" + e.getMessage() + "堆栈:" + Arrays.toString(e.getStackTrace()));
			return null;
		}
	}

	/**
	 * @param properties2
	 * @param config
	 */
	private static void replace(Properties properties, Map<String, String> config) {
		if (config != null) {
			for (String key : config.keySet()) {
				properties.put(key, config.get(key));
			}
		}
	}

	public static int BLACKLIST_THRESHOLD = 0;
	public static int BLACKLIST_PROCESS = 0;

	public static String get(final String key) {
		return properties.getProperty(key);
	}

	public static int getInt(final String key) {
		return Integer.parseInt(properties.getProperty(key));
	}
}
