package com.tenddata.collector.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.tendcloud.config.api.Configuration;
import com.tendcloud.controller.client.embed.Initer;
import com.tendcloud.controller.client.util.Config;
import com.tenddata.collector.Main;
import com.tenddata.collector.PushMain;

public class Init extends HttpServlet {
	
	private static final long serialVersionUID = -6025433566027740882L;

	@Override
	public void init() throws ServletException {
//		PropertyConfigurator.configureAndWatch(Thread.currentThread()
//				.getContextClassLoader()
//				.getResource("log4j.properties").getFile());
		
		Configuration conf = Configuration.getInstance();
		Config.broker = conf.getConfig("batch.jms.broker");
		Initer.init4DAGAsync();
		
		com.tenddata.collector.uuc.control.cas.RebootChecker.work();
		com.tenddata.collector.uuc.control.batch.BatchManager.getInstance();
		com.tenddata.collector.uuc.control.send.SendControl.work();
		
		new Thread() {
			public void run() {
				String domain = com.tenddata.collector.util.Configuration.get("domain");
				Main.run(domain);
			};
		}.start();
		
		new Thread() {
			public void run() {
				String domain = com.tenddata.collector.util.Configuration.get("domain");
				PushMain.run(domain);
			};
		}.start();
	}
	
}
