package com.tenddata.collector.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.talkingdata.messagesender.engine.MessageSender;

/**
 * <p>Datetime   : 2013-4-22 下午4:33:39</p>
 * <p>Title      : ErrorWatcher.java</p>
 * <p>Description: 监控发生的错误；报警用</p>
 * <p>Copyright  : Copyright (c) 2013</p>
 * <p>Company    : TendCloud</p>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class ErrorWatcher {
	
	private static int threshold;
	
	private static List<Integer> policy;
	
	private static int interval;
	
	private static int period;
	
	private static Map<String, AtomicLong> errors;
	
	private static ScheduledThreadPoolExecutor checker = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
	
	private static ArrayList<String> timerTasks = new ArrayList<String>();
	private static java.util.Timer timer = new java.util.Timer();
	
	private static ArrayList<String> periodCheckTasks = new ArrayList<String>();
	private static ScheduledThreadPoolExecutor periodChecker = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
	
	private static ErrorWatcher instance;
	
	private static String localInfo;
	
	private ErrorWatcher() {
		init();
	}
	
	public static ErrorWatcher getInstance() {
		if (instance == null) {
			synchronized (ErrorWatcher.class) {
				if (instance == null) {
					instance = new ErrorWatcher();
				}
			}
		}
		return instance;
	}
	
	private void init() {
		threshold = Configuration.getInt("error_watcher_threshold");
		interval = Configuration.getInt("error_watcher_interval");
		period = Configuration.getInt("error_watcher_period");
		policy = new ArrayList<Integer>();
		for (String p : Configuration.get("error_watcher_policy").split(",")) {
			policy.add(Integer.parseInt(p));
		}
		errors = new HashMap<String, AtomicLong>();
		
		checker.scheduleWithFixedDelay(new Checker(), interval, interval, TimeUnit.SECONDS);

		String dir = LocalInfor.Userdir;
		dir = dir.substring(dir.lastIndexOf(System.getProperty("file.separator")), dir.length());
		
		localInfo = LocalInfor.localip + dir;
	}

	public void add(String key) {
		if (!errors.containsKey(key)) {
			errors.put(key, new AtomicLong(0));
		}
		errors.get(key).incrementAndGet();
	}
	
	private static void sendMsg(String error, int pass, int count, int delay) {
		MessageSender.send(Configuration.get("error_watcher_phone"), localInfo + error + ". 过去的" + pass + "秒内，共发生" 
				+ count + "次。如果继续有此错误，下次报警将在" + delay + "秒后发送。");
		errors.get(error).set(0);
	}
	
	class Checker implements Runnable {
		public void run() {
			for (String key : errors.keySet()) {
				if (periodCheckTasks.contains(key)) {
					// 若长期检查中包括此项，直接跳过
					continue;
				} else if (timerTasks.contains(key)) {
					continue;
				} else {
					if (errors.get(key).intValue() / policy.get(0) >= threshold) {
						sendMsg(key, policy.get(0), errors.get(key).intValue(), policy.get(0));
						timer.schedule(new Task(key, 0), policy.get(0) * 1000);
						timerTasks.add(key);
					}
				}
			}
		}
	}
	
	class Task extends TimerTask {
		private String error;
		private int nowStage;
		
		public Task (String error, int nowStage) {
			this.error = error;
			this.nowStage = nowStage;
		}
		
		public void run() {
			if (nowStage + 1 < policy.size()) {
				if (errors.get(error).intValue() / period >= threshold) {
					timer.schedule(new Task(error, nowStage + 1), policy.get(nowStage + 1) * 1000);
					sendMsg(error, policy.get(nowStage), errors.get(error).intValue(), policy.get(nowStage + 1));
				} else {
					timerTasks.remove(error);
				}
			} else {
				if (errors.get(error).intValue() / period >= threshold) {
					sendMsg(error, policy.get(nowStage), errors.get(error).intValue(), period);
				}
				timerTasks.remove(error);
				if (periodChecker.isShutdown()) {
					periodChecker = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
				}
				periodChecker.scheduleWithFixedDelay(new PeriodChecker(error), period, period, TimeUnit.SECONDS);
				periodCheckTasks.add(error);
			}
		}
	}
	
	class PeriodChecker implements Runnable {
		private String error;
		
		public PeriodChecker(String error) {
			this.error = error;
		}
		
		public void run() {
			if (errors.get(error).intValue() / period >= threshold) {
				sendMsg(error, period, errors.get(error).intValue(), period);
			} else {
				periodCheckTasks.remove(error);
				if (periodCheckTasks.size() == 0) {
					periodChecker.shutdown();
				}
			}
		}
	}
	
}
