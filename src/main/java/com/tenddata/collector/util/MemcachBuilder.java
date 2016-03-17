package com.tenddata.collector.util;

import java.io.IOException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.KestrelCommandFactory;
import net.rubyeye.xmemcached.impl.RoundRobinMemcachedSessionLocator;
import net.rubyeye.xmemcached.utils.AddrUtil;

public class MemcachBuilder {
	private static MemcachBuilder instance = new MemcachBuilder();
	private MemcachedClientBuilder builder = null;
	private MemcachedClient client = null;

	private MemcachBuilder() {
	}
	
	public static MemcachBuilder getInstance() {
		return instance;
	}

	public MemcachedClient getClient() {
		return client;
	}
	
	public MemcachedClient createSession(String servers, int poolsize) {
		MemcachedClient client = null;
		
		System.out.println("Create kestrel session:[" + servers + "]");

		builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(servers.trim()));
		if (builder != null) {
			System.out.println("连接数量:[" + poolsize + "]");
			
//			builder.setFailureMode(false);
//			builder.setConnectionPoolSize(5); // set connection pool size to five
//	        builder.setSocketOption(StandardSocketOption.SO_KEEPALIVE, true);
//	        builder.setSocketOption(StandardSocketOption.SO_RCVBUF, 64 * 1024);
//	        builder.setSocketOption(StandardSocketOption.SO_SNDBUF, 64 * 1024);
//	        builder.setSocketOption(StandardSocketOption.SO_REUSEADDR, true);
//	        builder.setSocketOption(StandardSocketOption.TCP_NODELAY, false);
	        
			builder.setCommandFactory(new KestrelCommandFactory());
			builder.setSessionLocator(new RoundRobinMemcachedSessionLocator());
			builder.setConnectionPoolSize(poolsize);
			try {
				client = builder.build();
				client.setOpTimeout(2000L);
				this.client = client;
				return client;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}
	
}