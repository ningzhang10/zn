package com.tenddata.collector.util;

public class LocalInfor {
	public static String localip = "";
	public static int localport = 0;
	public static String Userdir = "";

	public static void setInfor(String local_ip,int local_port, String User_dir) {
		localport = local_port;
		localip = local_ip;
		Userdir = User_dir;
	}
}
