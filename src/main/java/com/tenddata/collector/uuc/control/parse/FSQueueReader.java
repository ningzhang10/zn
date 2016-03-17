package com.tenddata.collector.uuc.control.parse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.fqueue.FQueue;
import com.tendcloud.tenddata.entity.EventPackage;
import com.tenddata.collector.uuc.control.batch.BatchManager;
import com.tenddata.collector.uuc.fqueue.FSQueueManager;
import com.tenddata.util.stringUtil.Codeconvert;

/**
 * <p>Datetime   : 2013-4-17 下午5:49:42</p>
 * <p>Title      : FSQueueReader.java</p>
 * <p>Description: </p>
 * <p>Copyright  : Copyright (c) 2013</p>
 * <p>Company    : TendCloud</p>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class FSQueueReader implements Runnable {

	private static final Log log = LogFactory.getLog("CollectorServlet_UUC");
	
	public void run() {
		// 如果一个batch结束，正在处理，则暂停从fqueue中取数据
		if (BatchManager.getInstance().isNewBatchTime.get()) {
			return;
		}
		
		FQueue q = FSQueueManager.getInstance().getReceiveFQueue();
		
		EventPackage ep = null;
		int s = q.size();
		if (s <= 0) {
			return;
		}
		byte[] data = q.poll();
		
		ByteArrayInputStream bi = null;
		ObjectInputStream in = null;
		
		try {
			if (data != null && data.length != 0) {
				bi = new ByteArrayInputStream(data);
				in = new ObjectInputStream(bi);
				ep = (EventPackage) in.readObject();
				CollectorUtil.logEvent(ep, ep.ip, ep.rectime);
			}
		} catch (Exception e) {
			log.error("The data get from fqueue can't be unpacked. Raw data:["+ Codeconvert.ByteToStr(data, 0, data.length) + "]\r\n", e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (bi != null) {
				try {
					bi.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
