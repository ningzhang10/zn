/**
 * 
 * @author davy
 * 日期:		2013-5-23 11:17:49
 * 
 * The default character set is UTF-8.
 */
package com.tenddata.collector.util;

import org.apache.log4j.Logger;

/**
 * The Class StringUtils. The default character set is UTF-8
 * <p>
 * 中文： <br>
 * 这是org.apache.commons.lang.StringUtils的子类。 <br>
 * English: <br>
 * This is org.apache.commons.lang.StringUtils subclass.
 * </p>
 * 
 * @author davy
 */
public class StringUtils extends org.apache.commons.lang.StringUtils {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(StringUtils.class);

	/**
	 * 中文： <br>
	 * 如果原始字符串为空，返回空StringBuffer，否则删除最后一个要删除的字符串。 <br>
	 * English: <br>
	 * If the original string is empty, return an empty StringBuffer, or delete the last one you want to delete the
	 * string.
	 * 
	 * @param rawString
	 *            原始的字符串
	 * @param removeString
	 *            要删除的最后一个字符串
	 * @return 如果原始字符串为空，返回空StringBuffer，否则删除最后一个要删除的字符串
	 */
	public String notNullRemoveLast(String rawString, String removeString) {
		String string = new String();
		if (rawString != null) {
			if (Utils.isEmpty(removeString) || rawString.indexOf(removeString) == -1) {
				return rawString;
			} else if (rawString.lastIndexOf(removeString) + removeString.length() == rawString.length()) {
				string += rawString.substring(0, rawString.lastIndexOf(removeString));
			} else {
				string = rawString;
			}
		}
		return string;
	}

	/**
	 * 中文： <br>
	 * 如果原始字符串为空，返回空StringBuffer，否则添加一个要添加的字符串。<br>
	 * English: <br>
	 * If the original string is empty, return an empty StringBuffer, or add a string to be added.
	 * 
	 * @param rawString
	 *            原始的字符串
	 * @param addString
	 *            要添加一个字符串
	 * @return 如果原始字符串为空，返回空StringBuffer，否则添加一个要添加的字符串
	 */
	public StringBuffer notNullAdd(StringBuffer rawString, String addString) {
		StringBuffer stringBuffer = new StringBuffer();
		if (rawString != null) {
			if (Utils.isEmpty(addString)) {
				return rawString;
			} else {
				stringBuffer.append(rawString).append(addString);
			}
		}
		return stringBuffer;
	}

	/**
	 * Gets the value.
	 * 
	 * @param n
	 *            the n
	 * @return the value
	 */
	public Number getValue(Number n) {
		if (Utils.isNull(n)) {
			n = 0;
		}
		return n;
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的boolean类型的值，为空或者转成boolean有异常时返回false。<br>
	 * English:<br>
	 * Returns a value of type boolean value is empty or when transferred to a boolean abnormal returns false.
	 * </p>
	 * 
	 * @param booleanValue
	 *            the boolean value
	 * @return true, if successful
	 */
	public static boolean stringToBool(String booleanValue) {
		return stringToBool(booleanValue, false);
	}

	/**
	 * 中文：<br>
	 * 返回一个value的boolean类型的值，为空或者转成boolean有异常时返回defaultValue。<br>
	 * English:<br>
	 * Returns a value of type boolean value is empty or when transferred to a boolean abnormal returns defaultValue. *
	 * 
	 * @param booleanValue
	 *            the boolean value
	 * @param defaultValue
	 *            the default value
	 * @return true, if successful
	 */
	public static boolean stringToBool(String booleanValue, boolean defaultValue) {
		try {
			if (Utils.isNotEmpty(booleanValue))
				defaultValue = Boolean.valueOf(booleanValue);
		} catch (Exception e) {
			logger.error(Utils.toLog("stringToBool失败,booleanValue是", booleanValue));
		}
		return defaultValue;
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的int类型的值，为空或者解析成int有异常返回0。<br>
	 * English:<br>
	 * Returns a value of type int value is empty or parse an int abnormal returns 0.
	 * </p>
	 * 
	 * @param intValue
	 *            the int value
	 * @return the int
	 */
	public static int stringToInt(String intValue) {
		return stringToInt(intValue, 0);
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的int类型的值，为空或者解析成int有异常返回defaultValue。<br>
	 * English:<br>
	 * Returns a value of type int value is empty or parse an int abnormal returns defaultValue.
	 * </p>
	 * 
	 * 
	 * @param intValue
	 *            the int value
	 * @param defaultValue
	 *            the default value
	 * @return the int
	 */
	public static int stringToInt(String intValue, int defaultValue) {
		try {
			defaultValue = Integer.parseInt(intValue);
		} catch (Exception e) {
			logger.error(Utils.toLog(e));
		}
		return defaultValue;
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的long类型的值，为空或者解析成long有异常时返回0。<br>
	 * English:<br>
	 * Returns a value of type long value is empty or parse a long abnormal returns 0.
	 * </p>
	 * 
	 * @param longValue
	 *            the long value
	 * @return the long
	 */
	public static long stringToLong(String longValue) {
		return stringToLong(longValue, 0);
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的long类型的值，为空或者解析成long有异常时返回defaultValue。<br>
	 * English:<br>
	 * Returns a value of type long value is empty or parse a long abnormal returns defaultValue.
	 * </p>
	 * 
	 * @param longValue
	 *            the long value
	 * @param defaultValue
	 *            the default value
	 * @return the long
	 */
	public static long stringToLong(String longValue, long defaultValue) {
		try {
			defaultValue = Long.parseLong(longValue);
		} catch (Exception e) {
			logger.error(Utils.toLog(e));
		}
		return defaultValue;
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的double类型的值，为空或者解析成double有异常时返回0。<br>
	 * English:<br>
	 * Returns a value of type double value is empty or when parsed into double abnormal returns 0.
	 * </p>
	 * 
	 * @param doubleValue
	 *            the double value
	 * @return the double
	 */
	public static double stringDouble(String doubleValue) {
		return stringDouble(doubleValue, 0);
	}

	/**
	 * <p>
	 * 中文：<br>
	 * 返回一个value的double类型的值，为空或者解析成double有异常时返回defaultValue。<br>
	 * English:<br>
	 * Returns a value of type double value is empty or parsed into double abnormal returns defaultValue.
	 * </p>
	 * 
	 * 
	 * @param doubleValue
	 *            the double value
	 * @param defaultValue
	 *            the default value
	 * @return the double
	 */
	public static double stringDouble(String doubleValue, double defaultValue) {
		try {
			defaultValue = Double.parseDouble(doubleValue);
		} catch (Exception e) {
			logger.error(Utils.toLog(e));
		}
		return defaultValue;
	}

	/**
	 * * Returns the string representation of the <code>Object</code> argument.
	 * 
	 * @param obj
	 *            an <code>Object</code>.
	 * @return if the argument is <code>null</code>, then a string equal to <code>"null"</code>; otherwise, the value of
	 *         <code>obj.toString()</code> is returned.
	 * @see java.lang.Object#toString()
	 */
	public static String ToString(Object o) {
		return String.valueOf(o);
	}

	/**
	 * 中文： <br>
	 * 如果有多个字符串连接，使用此方法会提高效率。<br>
	 * English: <br>
	 * If there are multiple string concatenation, use this method to improve efficiency.
	 * 
	 * @param message
	 *            the message
	 * @return the string
	 */
	public static String arrayToString(Object... message) {
		if (message != null && message.length > 0) {
			StringBuffer buffer = new StringBuffer();
			for (Object string : message) {
				buffer.append(ToString(string));
			}
			return buffer.toString();
		}
		return null;
	}

}
