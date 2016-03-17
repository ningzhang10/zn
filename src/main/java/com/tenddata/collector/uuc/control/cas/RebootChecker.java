package com.tenddata.collector.uuc.control.cas;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.google.code.fqueue.FQueue;
import com.tenddata.collector.uuc.control.batch.FileName;
import com.tenddata.collector.uuc.fqueue.FSQueueManager;
import com.tenddata.collector.util.Configuration;

/**
 * <p>Datetime   : 2013-7-2 上午11:55:26</p>
 * <p>Title      : RebootChecker.java</p>
 * <p>Description: 检查未完成操作</p>
 * <p>Copyright  : Copyright (c) 2013</p>
 * <p>Company    : TendCloud</p>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class RebootChecker {

	public static void work() {
		new Thread() {
			public void run() {
				String scanDir = Configuration.get("batch.logger.t1.dir");
				File dir = new File(scanDir);
//				if (dir == null) {
//					dir.mkdir();
//				}
				
				if (dir.listFiles() == null) {
					return;
				}
				
				for (File f : dir.listFiles()) {
					if (FileName.getSuffix(f.getName()).equals(FileName.t1_uncomplete)) {
						int size = 0;
						try {
							BufferedReader br = new BufferedReader(new FileReader(f));
							while (br.readLine() != null) {
								size++;
							}
						} catch (FileNotFoundException e1) {
							e1.printStackTrace();
							continue;
						} catch (IOException e) {
							e.printStackTrace();
							continue;
						}
						
						if (size > 0) {
							// 将文件名写入event fqueue
							FQueue q = FSQueueManager.getInstance().getEventFQueue();
							try {
								q.offer(FileName.addSize(f.getCanonicalPath(), size).getBytes());
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			};
		}.start();
	}
}
