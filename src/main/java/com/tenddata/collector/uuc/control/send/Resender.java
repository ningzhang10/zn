package com.tenddata.collector.uuc.control.send;

import com.tenddata.collector.uuc.fqueue.FSQueueManager;

public class Resender implements Runnable {
	
	@Override
	public void run() {
		while (true) {
			byte[] data = FSQueueManager.getInstance().getResendFQueue().poll();
			if (data == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
					Sender sender = new Sender();
					
					String s = new String(data);
					String stats = s.substring(0, 1);
					String f = s.substring(1);
					
					if (stats.equals("1")) {
						sender.process(f.getBytes());
					} else if (stats.equals("2")) {
						try {
							sender.process(f.getBytes());
						} catch (Exception e) {
							e.printStackTrace();
							FSQueueManager.getInstance().getResendFQueue().offer(data);
						}
					}
			}
		}
	}
	
}
