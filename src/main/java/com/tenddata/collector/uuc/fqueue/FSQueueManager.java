package com.tenddata.collector.uuc.fqueue;

import com.google.code.fqueue.FQueue;
import com.tenddata.collector.util.Configuration;

/**
 * <p>Datetime   : 2013-4-17 下午4:43:37</p>
 * <p>Title      : FSqueueManager.java</p>
 * <p>Description: </p>
 * <p>Copyright  : Copyright (c) 2013</p>
 * <p>Company    : TendCloud</p>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class FSQueueManager {

	private FSQueueManager() {
		init(initReceiveQ | initResendQ | initEventQ);
	}
	
	private static FSQueueManager instance = null;
	
	public static FSQueueManager getInstance() {
		if (instance == null) {
			synchronized (FSQueueManager.class) {
				if (instance == null) {
					instance = new FSQueueManager();
				}
			}
		}
		return instance;
	}
	
	// 接收queue
	private FQueue receiveq;
	// 解析后的事件queue
	private FQueue eventq;
	// 失败重发queue
	private FQueue resendQ;
	
	private static final int initReceiveQ = 1;
	private static final int initEventQ = 2;
	private static final int initResendQ = 4;

	public void init(int flag) {
		try {
			if ((flag & initReceiveQ) == initReceiveQ && receiveq == null) {
				receiveq = new FQueue(Configuration.get("receivefq"), Configuration.getInt("fqueue.size"));
			}
			if ((flag & initEventQ) == initEventQ && eventq == null) {
				eventq = new FQueue(Configuration.get("eventfq"), Configuration.getInt("fqueue.size"));
			}
			if ((flag & initResendQ) == initResendQ && resendQ == null) {
				resendQ = new FQueue(Configuration.get("resendfq"), Configuration.getInt("fqueue.size"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public FQueue getReceiveFQueue() {
		if (receiveq == null) {
			synchronized (FSQueueManager.class) {
				if (receiveq == null) {
					init(initReceiveQ);
				}
			}
		}
		return receiveq;
	}
	
	public FQueue getEventFQueue() {
		if (eventq == null) {
			synchronized (FSQueueManager.class) {
				if (eventq == null) {
					init(initEventQ);
				}
			}
		}
		return eventq;
	}
	
	public FQueue getResendFQueue() {
		if (resendQ == null) {
			synchronized (FSQueueManager.class) {
				if (resendQ == null) {
					init(initResendQ);
				}
			}
		}
		return resendQ;
	}
	
	public long getReceiveFQueueSize() {
		return receiveq.size();
	}
	
	public long getEventFQueueSize() {
		return eventq.size();
	}
	
	public long getResendFQueueSize() {
		return resendQ.size();
	}
	
}
