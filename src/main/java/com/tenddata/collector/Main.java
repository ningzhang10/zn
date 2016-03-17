package com.tenddata.collector;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import com.tendcloud.tenddata.entity.EventPackage;
import com.tenddata.collector.uuc.control.parse.CollectorUtil;
import com.tenddata.collector.util.Configuration;
import com.tenddata.collector.util.MemcachBuilder;

public class Main {

	public static void run(String domain) {
		MemcachBuilder.getInstance().createSession(Configuration.get("memcache.server"), 5);
		ByteArrayInputStream bi = null;
		ObjectInputStream in = null;
		byte[] data = null;
		Object obj = null;
		EventPackage ep = null;
		
		while (true) {
			try {
				obj = MemcachBuilder.getInstance().getClient().get(domain);
				if (obj == null) {
					Thread.sleep(200);
					continue;
				}
				
				data = (byte[]) obj;
				
				if (data != null && data.length != 0) {
					bi = new ByteArrayInputStream(data);
					in = new ObjectInputStream(bi);
					ep = (EventPackage) in.readObject();
					CollectorUtil.logEvent2(ep, ep.ip, ep.rectime);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
