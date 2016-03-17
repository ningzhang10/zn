package com.tenddata.collector.uuc.control.batch;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.tenddata.collector.util.SingletonInstance;
import com.tenddata.collector.uuc.control.parse.Parsed4Package;
import com.tenddata.collector.uuc.control.parse.ParsedPackage;


public class BatchLogger {

	public static ReentrantLock lock = new ReentrantLock();
	
	public static void log2(Parsed4Package pp) {
		lock.lock();
		Map<String, FileWriter> writers = BatchManager.getInstance().writers;
		try {
			for (String device : pp.device) {
				if (device != null && !"".equals(device)) {
					SingletonInstance.getUucMetrics().getCounters().get("device").inc();
					writers.get("device").write(device);
					writers.get("device").write(System.getProperty("line.separator"));
				}
			}
			for (String launch : pp.launch) {
				if (launch != null && !"".equals(launch)) {
					SingletonInstance.getUucMetrics().getCounters().get("launch").inc();
					writers.get("launch").write(launch);
					writers.get("launch").write(System.getProperty("line.separator"));
				}
			}
			for (String activity : pp.activity) {
				if (activity != null && !"".equals(activity)) {
					SingletonInstance.getUucMetrics().getCounters().get("activity").inc();
					writers.get("activity").write(activity);
					writers.get("activity").write(System.getProperty("line.separator"));
				}
			}
			for (String appevent : pp.appevent) {
				if (appevent != null && !"".equals(appevent)) {
					SingletonInstance.getUucMetrics().getCounters().get("appevent").inc();
					writers.get("appevent").write(appevent);
					writers.get("appevent").write(System.getProperty("line.separator"));
				}
			}
			for (String keyvalue : pp.keyvalue) {
				if (keyvalue != null && !"".equals(keyvalue)) {
					SingletonInstance.getUucMetrics().getCounters().get("keyvalue").inc();
					writers.get("keyvalue").write(keyvalue);
					writers.get("keyvalue").write(System.getProperty("line.separator"));
				}
			}
			for (String exception : pp.exception) {
				if (exception != null && !"".equals(exception)) {
					SingletonInstance.getUucMetrics().getCounters().get("exception").inc();
					writers.get("exception").write(exception);
					writers.get("exception").write(System.getProperty("line.separator"));
				}
			}
			for (String terminate : pp.terminate) {
				if (terminate != null && !"".equals(terminate)) {
					SingletonInstance.getUucMetrics().getCounters().get("terminate").inc();
					writers.get("terminate").write(terminate);
					writers.get("terminate").write(System.getProperty("line.separator"));
				}
			}			
			for (String thirdID : pp.thirdID) {
				if (thirdID != null && !"".equals(thirdID)) {
					SingletonInstance.getUucMetrics().getCounters().get("thirdID").inc();
					writers.get("thirdID").write(thirdID);
					writers.get("thirdID").write(System.getProperty("line.separator"));
				}
			}			
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				BatchManager.getInstance().flushAll();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		lock.unlock();
	}
	public static void log(ParsedPackage pp) {
		lock.lock();
		Map<String, FileWriter> writers = BatchManager.getInstance().writers;
		try {
			for (String init : pp.init) {
				if (init != null && !"".equals(init)) {
					SingletonInstance.getUucMetrics().getCounters().get("init").inc();
					writers.get("init").write(init);
					writers.get("init").write(System.getProperty("line.separator"));
				}
			}
			for (String register : pp.register) {
				if (register != null && !"".equals(register)) {
					SingletonInstance.getUucMetrics().getCounters().get("register").inc();
					writers.get("register").write(register);
					writers.get("register").write(System.getProperty("line.separator"));
				}
			}
			for (String login : pp.login) {
				if (login != null && !"".equals(login)) {
					SingletonInstance.getUucMetrics().getCounters().get("login").inc();
					writers.get("login").write(login);
					writers.get("login").write(System.getProperty("line.separator"));
				}
			}
			for (String payreq : pp.payreq) {
				if (payreq != null && !"".equals(payreq)) {
					SingletonInstance.getUucMetrics().getCounters().get("payreq").inc();
					writers.get("payreq").write(payreq);
					writers.get("payreq").write(System.getProperty("line.separator"));
				}
			}
			for (String paysucc : pp.paysucc) {
				if (paysucc != null && !"".equals(paysucc)) {
					SingletonInstance.getUucMetrics().getCounters().get("paysucc").inc();
					writers.get("paysucc").write(paysucc);
					writers.get("paysucc").write(System.getProperty("line.separator"));
				}
			}
			if (pp.serverpaysucc != null && !"".equals(pp.serverpaysucc)) {
				SingletonInstance.getUucMetrics().getCounters().get("serverpaysucc").inc();
				writers.get("serverpaysucc").write(pp.serverpaysucc);
				writers.get("serverpaysucc").write(System.getProperty("line.separator"));
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				BatchManager.getInstance().flushAll();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		lock.unlock();
	}
}
