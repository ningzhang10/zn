/**
 * 
 * @author davy
 * 日期:		2013-6-4 10:15:50
 * 
 * The default character set is UTF-8.
 */
package com.tenddata.collector.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

/**
 * The Class ObjectUtils. The default character set is UTF-8
 * 
 * @author davy
 */
public class ObjectUtils extends Utils {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(ObjectUtils.class);

	/**
	 * <p>
	 * 对象与字节数组之间的转换工具类,要求传入的对象必须实现序列号接口.<br>
	 * Between the object and byte array conversion tool category, requiring the incoming object must implement the
	 * interface serial number.
	 * </p>
	 * 
	 * @param obj
	 *            the obj
	 * @return the byte[]
	 */

	/**
	 * 对象转换成字节数组,要求传入的对象必须实现序列号接口.<br>
	 * Object into a byte array, requiring incoming object must implement the interface to the serial number.
	 * 
	 * @param obj
	 * @return byte[]
	 */
	public static byte[] ObjectToByte(java.lang.Object obj) {
		byte[] bytes = null;
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);

			bytes = bo.toByteArray();

			bo.close();
			oo.close();
		} catch (Exception e) {
			logger.error(Utils.toLog(e, "请检查你传入的对象是否继承了Serializable接口"));
		}
		return bytes;
	}

	/**
	 * 字节数组转换成对象.<br>
	 * Byte array into an object.
	 * 
	 * @param bytes
	 *            the bytes
	 * @return Object 取得结果后强制转换成你存入的对象类型
	 */
	public static Object ByteToObject(byte[] bytes) {
		if (isEmpty(bytes))
			return null;
		java.lang.Object obj = null;
		try {
			ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
			ObjectInputStream oi = new ObjectInputStream(bi);

			obj = oi.readObject();

			bi.close();
			oi.close();
		} catch (Exception e) {
			logger.error(Utils.toLog(e));
		}
		return obj;
	}

}
