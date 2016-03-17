package com.tenddata.collector.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 * Datetime   ：2012-8-24 上午11:59:43<br>
 * Title      : GetIPv4.java<br>
 * Description: IP工具类<br>
 * Company    : Tend<br>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class IPUtil {

	/**
	 * 获取本机可用的除回环地址之外的可用IPv4地址
	 * @return 返回java获取的可用的除回环网卡中的第一个，至于到底是哪个，得看运气了···
	 * @throws SocketException
	 */
	public static String getIPv4ExcludeLo() throws SocketException {
		String ip = "";
		
		Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
		for (NetworkInterface ni : Collections.list(en)) {
			
			if (!ni.isUp() || ni.isLoopback()) {
            	continue;
            }
            
            List<InterfaceAddress> list = ni.getInterfaceAddresses();
    		for (InterfaceAddress ia : list) {
    			InetAddress addr = ia.getAddress();
    			if (addr instanceof Inet4Address) {
    				ip = addr.getHostAddress();
    				return ip;
    			}
    		}
		}
		
        return ip;
	}

	public static String getClientIpAddr(HttpServletRequest request) {
        String ip = request.getHeader("x-forwarded-for");
        
        if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        
        return ip;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(getIPv4ExcludeLo());
		
		/*
		Enumeration<NetworkInterface> en = NetworkInterface
				.getNetworkInterfaces();
		while (en.hasMoreElements()) {
			NetworkInterface ni = en.nextElement();
			printParameter(ni);
		}*/
	}

	@SuppressWarnings("unused")
	private static void printParameter(NetworkInterface ni)
			throws SocketException {
		System.out.println(" Name = " + ni.getName());
		System.out.println(" Display Name = " + ni.getDisplayName());
		System.out.println(" Is up = " + ni.isUp());
		System.out.println(" Support multicast = " + ni.supportsMulticast());
		System.out.println(" Is loopback = " + ni.isLoopback());
		System.out.println(" Is virtual = " + ni.isVirtual());
		System.out.println(" Is point to point = " + ni.isPointToPoint());
		System.out.println(" Hardware address = " + Arrays.toString(ni.getHardwareAddress()));
		System.out.println(" MTU = " + ni.getMTU());

		System.out.println("\nList of Interface Addresses:");
		List<InterfaceAddress> list = ni.getInterfaceAddresses();

		for (InterfaceAddress ia : list) {
			System.out.println(" Address = " + ia.getAddress());
			System.out.println(" Broadcast = " + ia.getBroadcast());
			System.out.println(" Network prefix length = "
					+ ia.getNetworkPrefixLength());
			System.out.println("");
		}
		System.out.println("====");
	}
}
