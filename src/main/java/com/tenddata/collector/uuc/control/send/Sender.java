package com.tenddata.collector.uuc.control.send;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import td.olap.commons.hdfs.storage.HDFSReader;
import td.olap.commons.hdfs.storage.HDFSWriter;

import com.google.code.fqueue.FQueue;
import com.tendcloud.batch.FeedBack;
import com.tendcloud.controller.client.dag.ThisNode;
import com.tendcloud.controller.client.util.Config;
import com.tendcloud.controller.client.util.JMSConnection;
import com.tendcloud.controller.client.util.JacksonMapper;
import com.tendcloud.controller.client.util.Utils;
import com.tenddata.collector.util.Configuration;
import com.tenddata.collector.uuc.control.batch.FileName;
import com.tenddata.collector.uuc.fqueue.FSQueueManager;

public class Sender implements Runnable {

	@Override
	public void run() {
		while (true) {
			FQueue q = FSQueueManager.getInstance().getEventFQueue();
			byte[] data = q.poll();

			if (data == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				String ret = process(data);
				if (ret != null) {
					FSQueueManager.getInstance().getResendFQueue().offer(ret.getBytes());
				}
			}
		}
	}

	public String process(byte[] data) {
		String f = new String(data);
		File t1 = new File(f);

		int size = FileName.getSize(t1.getName());
		if (size == 0||!t1.exists()||t1.length()<=0) {
			t1.delete();
			return null;
		}

		String domain = Configuration.get("domain");
		String dagName = Configuration.get("dagname");
		String dagNode = Configuration.get("dagnode");
		String taskid = Utils.uuid();
		String hadoop_dir = Configuration.get("hadoop.splitter.dir");
		String addr = "";
		if(hadoop_dir != null && !hadoop_dir.equalsIgnoreCase("")){
			addr = hadoop_dir.trim();
		}
		String outAddr = Utils.assemOutAddr(addr+domain, ThisNode.clustertype, dagName, FileName.getEvent(t1.getName()), taskid);
		
		try {
			write2hdfs(t1, outAddr);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return "1" + f;
		}

		FeedBack feedback = new FeedBack();
		feedback.setPtaskid("root");
		feedback.setTaskid(taskid);
		feedback.setAddr(outAddr);
		feedback.setProduceTime(Utils.nowString());
		feedback.setSize(size);

		Session session = null;
		Connection con = null;
		try {
			for (int i = 0; i < 3; i++) {
				try {
					con = JMSConnection.getJMSConnection(Config.broker);
					session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
					break;
				} catch (org.apache.activemq.ConnectionFailedException e) {
					System.out.println(new Date());
					e.printStackTrace();
					con = JMSConnection.renewJMSConnection(Config.broker);
					System.out.println(new Date()+"第"+i+"次重置了连接");
				}
			}
			MessageProducer producer = session.createProducer(session.createQueue("task_profile"));
			String json = JacksonMapper.getObjectMapper().writeValueAsString(feedback);
			Message msg = session.createTextMessage(json);
			msg.setStringProperty("domain", domain);
			msg.setStringProperty("dagName", dagName);
			msg.setStringProperty("producerId", ThisNode.nodeId);
			msg.setStringProperty("dagNode", dagNode); // 即dag中的nodeid
			msg.setStringProperty("outputNext", "etl." + FileName.getEvent(t1.getName())); // 即dag中的nodeid
			// msg.setStringProperty("dagNode",FileName.getEvent(t1.getName()));
			// // TODO: 改成这样或许更好
			producer.send(msg);
			t1.delete();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// if (con != null) {
			// try {
			// con.close();
			// } catch (JMSException e) {
			// e.printStackTrace();
			// }
			// }
		}

		return null;
	}

	public void write2hdfs(File file, String hdfsPath) throws FileNotFoundException, IOException {
		HDFSWriter hw = new HDFSWriter(hdfsPath + file.getName());
		hw.write(new FileInputStream(file), true);
		HDFSReader reader = new HDFSReader(hdfsPath, false);		
		System.out.println("zn: "+reader.readLine());

		// t2
		// String nextStage = FileName.rename(file.getName(), FileName.t1_over,
		// FileName.t2);
		// File t2File = new File(file.getParent(), nextStage);
		// file.renameTo(t2File);
	}	
}
