package com.tenddata.collector.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tenddata.collector.uuc.fqueue.FSQueueManager;

public class Stats extends HttpServlet {

	private static final long serialVersionUID = -71436602305738582L;

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		out.write("recive fqueue size: " + FSQueueManager.getInstance().getReceiveFQueueSize());
		out.write("\r\n");
		out.write("event fqueue size: " + FSQueueManager.getInstance().getEventFQueueSize());
		out.write("\r\n");
		out.write("resend fqueue size: " + FSQueueManager.getInstance().getResendFQueueSize());
		out.write("\r\n");
	}
	
}
