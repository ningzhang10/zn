/**
 * 
 * @author davy
 * 日期:		2013-3-28 17:16:21
 * 
 * The default character set is UTF-8.
 */
package com.tenddata.collector.util;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The Class Utils. The default character set is UTF-8
 * 
 * @author davy 日期: 2013-3-22 下午3:36:19
 */
public class Utils implements Serializable {

	/**
	 * 中文： <br>
	 * 如果t为null,返回true,否则返回false。<br>
	 * English: <br>
	 * If t is null, returns true, otherwise returns false.
	 * 
	 * @param t
	 *            任意的一个对象
	 * @return 如果t为null,返回true,否则返回false;
	 */
	public static boolean isNull(Object t) {
		return t == null;
	}

	/**
	 * 中文： <br>
	 * 如果t不为null,返回true,否则返回false。<br>
	 * English: <br>
	 * If t is not null, returns true, otherwise returns false.
	 * 
	 * @param t
	 *            the t
	 * @return true, if is not null
	 */
	private static boolean isNotNull(Object t) {
		return !isNull(t);
	}

	/**
	 * 中文： <br>
	 * 如果t都不为null,返回true,否则返回false。<br>
	 * 例如<br>
	 * A和B和C<br>
	 * 如果A和B和C都不为null，那么isNotNull(A,B,C)返回true，否则返回false。<br>
	 * English: <br>
	 * If t is not is null, returns true, otherwise returns false. <br>
	 * For example <br>
	 * A and B and C <br>
	 * If A and B and C are not as null, then isNotNull (A, B, C) returns true, otherwise returns false.
	 * 
	 * @param t
	 *            the t
	 * @return true, if is not null
	 */
	public static boolean isNotNull(Object... t) {
		boolean flag = true;
		for (Object object : t) {
			flag = flag && isNotNull(object);
		}
		return flag;
	}

	/**
	 * 中文： <br>
	 * 如果t为empty,返回true,否则返回false。 <br>
	 * 如果t是String。
	 * <p>
	 * Checks if a String is whitespace, empty ("") or null.
	 * </p>
	 * 
	 * <pre>
	 *       StringUtils.isBlank(null)      = true
	 *       StringUtils.isBlank("")        = true
	 *       StringUtils.isBlank(" ")       = true
	 *       StringUtils.isBlank("bob")     = false
	 *       StringUtils.isBlank("  bob  ") = false
	 * </pre>
	 * 
	 * <br>
	 * 如果是List，为null或者size等于0为true。 <br>
	 * 如果是Map，为null或者size等于0为true。 <br>
	 * 如果是Long、Integer、Double、Float、Byte、Short 为null或者等于0为true。 <br>
	 * English: <br>
	 * If t is empty, returns true, otherwise returns false. <br>
	 * If t is String.
	 * <p>
	 * Checks if a String is whitespace, empty ("") or null.
	 * </ p>
	 * 
	 * <pre>
	 *        StringUtils.isBlank (null) = true
	 *        StringUtils.isBlank ("") = true
	 *        StringUtils.isBlank ("") = true
	 *        StringUtils.isBlank ("bob") = false
	 *        StringUtils.isBlank ("bob") = false
	 *       
	 * </pre>
	 * 
	 * <br>
	 * If yes List, is null or size equal to 0 to true. <br>
	 * If Map, is null or size equal to 0 to true. <br>
	 * If Long, Integer, Double, Float, Byte, Short Is null or equal to 0 to true. *
	 * 
	 * @param t
	 *            the t
	 * @return true, if is empty
	 */
	public static boolean isEmpty(Object t) {
		if (isNull(t))
			return true;
		if (t instanceof String)
			return StringUtils.isBlank((String) t);
		if (t instanceof List)
			return ((List) t).size() < 1;
		if (t instanceof Map)
			return ((Map) t).size() < 1;
		if (t instanceof Long)
			return ((Long) t) == 0;
		if (t instanceof Integer)
			return ((Integer) t) == 0;
		if (t instanceof Double)
			return ((Double) t) == 0;
		if (t instanceof Float)
			return ((Float) t) == 0;
		if (t instanceof Byte)
			return ((Byte) t) == 0;
		if (t instanceof Short)
			return ((Short) t) == 0;
		return false;
	}

	/**
	 * 中文： <br>
	 * 如果t不为empty,返回true,否则返回false。<br>
	 * English: <br>
	 * If t is not empty, returns true, otherwise returns false. *
	 * 
	 * @param t
	 *            the t
	 * @return true, if is not empty
	 */
	private static boolean isNotEmpty(Object t) {
		return !isEmpty(t);
	}

	/**
	 * 中文： <br>
	 * 如果t都不为empty,返回true,否则返回false。<br>
	 * 例如<br>
	 * A和B和C<br>
	 * 如果A和B和C都不为empty，那么isNotEmpty(A,B,C)返回true，否则返回false。<br>
	 * English: <br>
	 * If t is not as empty, returns true, otherwise returns false. <br>
	 * For example <br>
	 * A and B and C <br>
	 * If A and B and C are not as empty, then isNotEmpty (A, B, C) returns true, otherwise returns false. *
	 * 
	 * @param t
	 *            the t
	 * @return true, if is not empty
	 */
	public static boolean isNotEmpty(Object... t) {
		boolean flag = true;
		for (Object object : t) {
			flag = flag && isNotEmpty(object);
		}
		return flag;
	}

	/**
	 * Returns the string representation of the <code>Object</code> argument.
	 * 
	 * @param obj
	 *            an <code>Object</code>.
	 * @return if the argument is <code>null</code>, then a string equal to <code>"null"</code>; otherwise, the value of
	 *         <code>obj.toString()</code> is returned.
	 * @see java.lang.Object#toString()
	 */
	public String toString(Object t) {
		String returnStr = null;
		if (isNotNull(t)) {
			returnStr = t.toString();
		}
		return returnStr;
	}

	/**
	 * 中文：<br>
	 * 返回系统当前时间的Date。 <br>
	 * English: <br>
	 * Return the current system time Date.
	 * 
	 * @return the date
	 */
	public static Date now() {
		return new Date();
	}

	/**
	 * 中文：<br>
	 * 返回系统当前时间的格式化后的字符串。 <br>
	 * English: <br>
	 * Return the current system time of the formatted string.
	 * 
	 * @param pattern
	 *            the pattern
	 * @return the string
	 */
	public static String now(String pattern) {
		Date date = now();
		SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
		return dateFormat.format(date);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see td.etl.commons.util.StringUtils#arrayToString(java.lang.String)
	 */
	public static String toLog(Object... message) {
		return StringUtils.arrayToString(message);
	}

	/**
	 * return td.etl.commons.util.StringUtils.arrayToString(message, toLog(e));
	 * 
	 * @param e
	 *            the e
	 * @param message
	 *            the message
	 * @return the string
	 */
	public static String toLog(Exception e, Object... message) {
		return StringUtils.arrayToString(message, toLog(e));
	}

	/**
	 * return e == null ? "异常为空" : "信息:" + e.getMessage() + "堆栈:" + Arrays.toString(e.getStackTrace());
	 * 
	 * @param e
	 *            the e
	 * @return the string
	 */
	public static String toLog(Exception e) {
		return e == null ? "异常为空" : "信息:" + e.getMessage() + "堆栈:" + Arrays.toString(e.getStackTrace());
	}
	public static String getDevice(String device) {
		if(device == null || device.equals("")){
			return "";
		}else if(device.length() > 200){
			return device.substring(0, 200);
		}else{
			return device;
		}
		
	}
	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {
		System.out.println(isNotNull(1, 2, 3));
	}

}
