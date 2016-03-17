package com.tenddata.collector.uuc.control.send;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class SendControl {

	private static ThreadPoolExecutor sendExec = (ThreadPoolExecutor) Executors.newFixedThreadPool(7);
	private static ThreadPoolExecutor resendExec = (ThreadPoolExecutor) Executors.newFixedThreadPool(7);
	
	public static void work() {
		sendExec.execute(new Sender());
		resendExec.execute(new Resender());
	}
	
}
