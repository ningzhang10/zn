package com.tenddata.collector.uuc.control.batch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.fqueue.FQueue;
import com.tenddata.collector.util.Configuration;
import com.tenddata.collector.util.SingletonInstance;
import com.tenddata.collector.uuc.fqueue.FSQueueManager;

public class BatchManager implements Runnable {

	public AtomicBoolean isNewBatchTime = new AtomicBoolean(false);

	public HashMap<String, FileWriter> writers = new HashMap<String, FileWriter>();
	private HashMap<String, File> files = new HashMap<String, File>();
	
	private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
	
	private static BatchManager instance = new BatchManager();
	
	private BatchManager() {
		init();
	}
	
	private void init() {
		// regist counters
		String events = Configuration.get("batch.logger.t1.events");
		for (String event : events.split(",")) {
			SingletonInstance.getUucMetrics().counter(event);
		}
		
		// 按时间分隔文件
		int delay = Configuration.getInt("batch.logger.t1.threshold");
		exec.scheduleWithFixedDelay(this, 0, delay, TimeUnit.SECONDS);
	}

	public static BatchManager getInstance() {
		return instance;
	}
	
	@Override
	public void run() {
		isNewBatchTime.set(true);
		BatchLogger.lock.lock();
		if (writers.size() > 0) {
			try {
				// 关闭文件流
				closeAll();
			} catch (IOException e) {
				e.printStackTrace();
			}
			// 修改后缀、计数器归零
			t1over();
		}
		// 将文件名写入event fqueue
		FQueue q = FSQueueManager.getInstance().getEventFQueue();
		for (File file : files.values()) {
			try {
				q.offer((file.getCanonicalPath()).getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// 创建新文件
		createFiles();
		BatchLogger.lock.unlock();
		isNewBatchTime.set(false);
	}
	
	private void createFiles() {
		String events = Configuration.get("batch.logger.t1.events");
		String dir = Configuration.get("batch.logger.t1.dir");
		File fileDir = new File(dir);
		if (fileDir.exists() == false && fileDir.isDirectory() == false) {
			if (fileDir.mkdirs() == false) {
				System.err.println("create dir error");
				return;
			}
		}
		String batchid = BatchUtil.getBatchFileNamePrefix();
		for (String event : events.split(",")) {
			try {
				File file = new File(dir + FileName.getFileName(event, batchid, FileName.t1_uncomplete, 0));
				file.createNewFile();
				files.put(event, file);
				FileWriter fw = new FileWriter(file, true);
				writers.put(event, fw);
				file.createNewFile();
//				fw.write(Configuration.get("collector2etl." + event));
//				fw.write(System.getProperty("line.separator"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void flushAll() throws IOException {
		for (FileWriter fw : writers.values()) {
			fw.flush();
		}
	}
	
	private void closeAll() throws IOException {
		for (FileWriter fw : writers.values()) {
			fw.close();
		}
	}
	
	private void t1over() {
		// change suffix to t1over
		for (String event : files.keySet()) {
			long size = SingletonInstance.getUucMetrics().getCounters().get(event).getCount();
			SingletonInstance.getUucMetrics().getCounters().get(event).dec(size);
			
			File file = files.get(event);
			String name = file.getName();
			File newFile = new File(file.getParent(), FileName.t1over(name, size));
			file.renameTo(newFile);
			files.put(event, newFile);
		}
	}
	
}
