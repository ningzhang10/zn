package com.tenddata.collector;

import com.tenddata.collector.util.Configuration;
import com.tenddata.collector.util.MemcachBuilder;
import com.tenddata.collector.uuc.control.batch.BatchLogger;
import com.tenddata.collector.uuc.control.parse.EventSpliter;
import com.tenddata.collector.uuc.control.parse.ParsedPackage;

public class PushMain {

	public static void run(String domain) {
		MemcachBuilder.getInstance().createSession(Configuration.get("memcache.server"), 5);
		byte[] data = null;
		Object obj = null;

		while (true) {
			try {
				obj = MemcachBuilder.getInstance().getClient().get(domain + "_push");
				if (obj == null) {
					Thread.sleep(200);
					continue;
				}

				data = (byte[]) obj;

				if (data != null && data.length != 0) {
					ParsedPackage pp = new ParsedPackage();
					pp.serverpaysucc = EventSpliter.getServerPaysucc(new String(data), String.valueOf(System.currentTimeMillis()));

					BatchLogger.log(pp);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
