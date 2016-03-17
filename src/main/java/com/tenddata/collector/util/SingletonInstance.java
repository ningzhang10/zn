package com.tenddata.collector.util;

import org.msgpack.MessagePack;

import com.codahale.metrics.MetricRegistry;

/**
 * <p>Datetime   : 2013-4-18 下午5:15:05</p>
 * <p>Title      : SingletonInstance.java</p>
 * <p>Description: </p>
 * <p>Copyright  : Copyright (c) 2013</p>
 * <p>Company    : TendCloud</p>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class SingletonInstance {

	private static MessagePack messagepack = null;
	
	public static MessagePack getMessagePack() {
		if (messagepack == null) {
			synchronized (SingletonInstance.class) {
				if (messagepack == null) {
					messagepack = new MessagePack();
				}
			}
		}
		return messagepack;
	}
	
	private static MetricRegistry metrics_ipay = null;
	
	public static MetricRegistry getIpayMetrics() {
		if (metrics_ipay == null) {
			synchronized (SingletonInstance.class) {
				if (metrics_ipay == null) {
					metrics_ipay = new MetricRegistry();
				}
			}
		}
		return metrics_ipay;
	}
	
	private static MetricRegistry metrics_uuc = null;
	
	public static MetricRegistry getUucMetrics() {
		if (metrics_uuc == null) {
			synchronized (SingletonInstance.class) {
				if (metrics_uuc == null) {
					metrics_uuc = new MetricRegistry();
				}
			}
		}
		return metrics_uuc;
	}
}
