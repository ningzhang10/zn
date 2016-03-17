package com.tenddata.collector.uuc.control.parse;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.tendcloud.tenddata.entity.AppEvent;
import com.tendcloud.tenddata.entity.AppProfile;
import com.tendcloud.tenddata.entity.DeviceProfile;
import com.tendcloud.tenddata.entity.EventPackage;
import com.tendcloud.tenddata.entity.Session;
import com.tendcloud.tenddata.entity.TMessage;
import com.tenddata.collector.util.JacksonMapper;
import com.tenddata.collector.util.MyStringBuffer;
import com.tenddata.kestrel.hadoopbean.HadoopLaunch;

/**
 * <p>
 * Datetime : 2013-6-24 下午5:02:03
 * </p>
 * <p>
 * Title : EventSpliter.java
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright : Copyright (c) 2013
 * </p>
 * <p>
 * Company : TendCloud
 * </p>
 * 
 * @author <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class EventSpliter {
	
	public static String getBaseDate(EventPackage eventpackage,TMessage tmessage,String ip,AppEvent appevent) {
		MyStringBuffer sb = new MyStringBuffer();
		
		String networkOperator = eventpackage.mDeviceProfile.mNetworkOperator;
		String simOperator = eventpackage.mDeviceProfile.mSimOperator;
		String mcc = "";
		String mnc ="";
		if (!StringUtils.isEmpty(networkOperator) && networkOperator.length() >= 5) {
			String mccmnc = networkOperator.split(",")[0];
			mcc=mccmnc.substring(0, 3);	// mcc 前三位
			mnc=mccmnc.substring(3);	// mnc 三位以后
		} else if (!StringUtils.isEmpty(simOperator) && simOperator.length() >= 5) {
			String mccmnc = simOperator.split(",")[0];
			mcc=mccmnc.substring(0, 3);	// mcc 前三位
			mnc=mccmnc.substring(3);	// mnc 三位以后
		}
		String channel = "";
		//接入类型
		if(tmessage.session.isConnected == -1){
			channel = "-1";
		}else{
			channel = String.valueOf(eventpackage.mDeviceProfile.mChannel);
		}
		sb.append(eventpackage.mDeveploperAppkey);
		sb.append(CollectorUtil.getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
		sb.append(eventpackage.mAppProfile.mAppVersionCode);
		sb.append(eventpackage.mAppProfile.mAppVersionName);
		sb.append(eventpackage.mAppProfile.mPartnerId);
		sb.append(tmessage.session.start);
		sb.append(eventpackage.mDeviceProfile.mMobileModel);
		sb.append(eventpackage.mDeviceProfile.mOsVersion);
		sb.append(eventpackage.mDeviceProfile.mOsSdkVersion);
		sb.append(mcc);
		sb.append(mnc);
		sb.append(eventpackage.mDeviceProfile.mCarrier);
		sb.append(eventpackage.mDeviceProfile.mPixelMetric);
		sb.append(ip);
		sb.append(eventpackage.mDeviceProfile.mCountry);
		sb.append(channel);
		sb.append(tmessage.session.id);	
		sb.append(appevent.id);	
		sb.appendEnd(appevent.label);
		return sb.toString();	
	}
	public static String getInit(EventPackage ep, AppEvent appevent, Session session, String rectime) {
		try {
			MyStringBuffer sb = new MyStringBuffer();
			sb.append(ep.mDeveploperAppkey);
			sb.append(getPlatformCode(ep.mDeviceProfile, ep.mAppProfile));
			sb.append(ep.mDeviceId);
			sb.append(ep.mAppProfile.mPartnerId);
			sb.append(session.id);
			sb.append(session.start);
			sb.append(ep.mDeviceProfile.mMobileModel);
			sb.append(ep.mDeviceProfile.mOsSdkVersion);
			sb.append(ep.mDeviceProfile.mOsVersion);
			sb.append(ep.mDeviceProfile.mCarrier);
			sb.append(ep.mDeviceProfile.mSimOperator);
			sb.append(ep.mDeviceProfile.mNetworkOperator);
			sb.append(ep.mDeviceProfile.mPixelMetric);
			sb.append(session.isConnected == 0 ? false : true);
			sb.append(ep.mDeviceProfile.mChannel);
			sb.append(ep.mAppProfile.mAppPackageName);
			sb.append(ep.mAppProfile.appStoreID);
			sb.appendEnd(rectime);
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getLogin(EventPackage ep, AppEvent appevent, Map<String, Object> kvs, Session session, String rectime) {
		try {
			MyStringBuffer sb = new MyStringBuffer();
			sb.append(ep.mDeveploperAppkey);
			sb.append(getPlatformCode(ep.mDeviceProfile, ep.mAppProfile));
			sb.append(String.valueOf(kvs.get("uId")));
			sb.append(ep.mDeviceId);
			sb.append(ep.mAppProfile.mPartnerId);
			sb.append(session.id);
			sb.append(session.start);
			sb.append(ep.mDeviceProfile.mMobileModel);
			sb.append(ep.mDeviceProfile.mOsSdkVersion);
			sb.append(ep.mDeviceProfile.mOsVersion);
			sb.append(ep.mDeviceProfile.mCarrier);
			sb.append(ep.mDeviceProfile.mSimOperator);
			sb.append(ep.mDeviceProfile.mNetworkOperator);
			sb.append(ep.mDeviceProfile.mPixelMetric);
			sb.append(session.isConnected == 0 ? false : true);
			sb.append(ep.mDeviceProfile.mChannel);
			sb.append(ep.mAppProfile.mAppPackageName);
			sb.append(ep.mAppProfile.appStoreID);
			sb.appendEnd(rectime);
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getPaysucc(EventPackage ep, AppEvent appevent, Map<String, Object> kvs, Session session, String rectime) {
		try {
			MyStringBuffer sb = new MyStringBuffer();
			sb.append(ep.mDeveploperAppkey);
			sb.append(getPlatformCode(ep.mDeviceProfile, ep.mAppProfile));
			sb.append(String.valueOf(kvs.get("uId")));
			sb.append(ep.mDeviceId);
			sb.append(ep.mAppProfile.mPartnerId);
			sb.append(session.id);
			sb.append(session.start);
			sb.append(ep.mDeviceProfile.mMobileModel);
			sb.append(ep.mDeviceProfile.mOsSdkVersion);
			sb.append(ep.mDeviceProfile.mOsVersion);
			sb.append(ep.mDeviceProfile.mCarrier);
			sb.append(ep.mDeviceProfile.mSimOperator);
			sb.append(ep.mDeviceProfile.mNetworkOperator);
			sb.append(ep.mDeviceProfile.mPixelMetric);
			sb.append(session.isConnected == 0 ? false : true);
			sb.append(ep.mDeviceProfile.mChannel);
			sb.append(ep.mAppProfile.mAppPackageName);
			sb.append(ep.mAppProfile.appStoreID);
			sb.append(rectime);
			sb.append(String.valueOf(kvs.get("waresId")));
			sb.append(String.valueOf(kvs.get("goods")));
			sb.append(String.valueOf(kvs.get("quantity")));
			sb.append(String.valueOf(kvs.get("price")));
			sb.append(String.valueOf(kvs.get("feeType")));
			sb.append(String.valueOf(kvs.get("payType")));
			sb.appendEnd(String.valueOf(kvs.get("orderId")));
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getPayreq(EventPackage ep, AppEvent appevent, Map<String, Object> kvs, Session session, String rectime) {
		try {
			MyStringBuffer sb = new MyStringBuffer();
			sb.append(ep.mDeveploperAppkey);
			sb.append(getPlatformCode(ep.mDeviceProfile, ep.mAppProfile));
			sb.append(String.valueOf(kvs.get("uId")));
			sb.append(ep.mDeviceId);
			sb.append(ep.mAppProfile.mPartnerId);
			sb.append(session.id);
			sb.append(session.start);
			sb.append(ep.mDeviceProfile.mMobileModel);
			sb.append(ep.mDeviceProfile.mOsSdkVersion);
			sb.append(ep.mDeviceProfile.mOsVersion);
			sb.append(ep.mDeviceProfile.mCarrier);
			sb.append(ep.mDeviceProfile.mSimOperator);
			sb.append(ep.mDeviceProfile.mNetworkOperator);
			sb.append(ep.mDeviceProfile.mPixelMetric);
			sb.append(session.isConnected == 0 ? false : true);
			sb.append(ep.mDeviceProfile.mChannel);
			sb.append(ep.mAppProfile.mAppPackageName);
			sb.append(ep.mAppProfile.appStoreID);
			sb.append(rectime);
			sb.append(String.valueOf(kvs.get("waresId")));
			sb.append(String.valueOf(kvs.get("goods")));
			sb.append(String.valueOf(kvs.get("quantity")));
			sb.append(String.valueOf(kvs.get("price")));
			sb.append(String.valueOf(kvs.get("feeType")));
			sb.append(String.valueOf(kvs.get("payType")));
			sb.appendEnd(String.valueOf(kvs.get("orderId")));
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getRegiste(EventPackage ep, AppEvent appevent, Map<String, Object> kvs, Session session, String rectime) {
		try {
			MyStringBuffer sb = new MyStringBuffer();
			sb.append(ep.mDeveploperAppkey);
			sb.append(getPlatformCode(ep.mDeviceProfile, ep.mAppProfile));
			sb.append(String.valueOf(kvs.get("uId")));
			sb.append(ep.mDeviceId);
			sb.append(ep.mAppProfile.mPartnerId);
			sb.append(session.id);
			sb.append(session.start);
			sb.append(ep.mDeviceProfile.mMobileModel);
			sb.append(ep.mDeviceProfile.mOsSdkVersion);
			sb.append(ep.mDeviceProfile.mOsVersion);
			sb.append(ep.mDeviceProfile.mCarrier);
			sb.append(ep.mDeviceProfile.mSimOperator);
			sb.append(ep.mDeviceProfile.mNetworkOperator);
			sb.append(ep.mDeviceProfile.mPixelMetric);
			sb.append(session.isConnected == 0 ? false : true);
			sb.append(ep.mDeviceProfile.mChannel);
			sb.append(ep.mAppProfile.mAppPackageName);
			sb.append(ep.mAppProfile.appStoreID);
			sb.appendEnd(rectime);
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getServerPaysucc(String msg, String rectime) {
		try {
			MyStringBuffer sb = new MyStringBuffer();
			@SuppressWarnings("unchecked")
			com.tenddata.collector.bean.Map<String, Object> map = JacksonMapper.getObjectMapper().readValue(msg, com.tenddata.collector.bean.Map.class);
			sb.append(map.getStringValue("appid",""));
			sb.append(map.getStringValue("channelid",""));
			sb.append(map.getStringValue("deviceId",""));
			sb.append(map.getStringValue("userid",""));
			sb.append(map.getLongValue("timestamp"));
			sb.append(map.getStringValue("waresid",""));
			sb.append(map.getIntValue("quantity"));
			sb.append(map.getIntValue("rmb"));
			sb.append(map.getIntValue("virtualcurrency"));
			sb.append(map.getStringValue("feetype",""));
			sb.append(map.getStringValue("paytype",""));
			sb.append(map.getStringValue("orderid",""));
			sb.appendEnd(rectime);
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private static String getPlatformCode(DeviceProfile devProfile, AppProfile appProfile) {
		String retval = "0";

		String mSdkVersion = appProfile.mSdkVersion.toLowerCase();
		if (mSdkVersion.indexOf("android") != -1) {
			retval = "1";
		}

		if (mSdkVersion.indexOf("ios") != -1) {
			retval = "2";
		}

		if (mSdkVersion.indexOf("wphone") != -1) {
			retval = "4";
		}

		return retval;
	}

	public static void main(String[] args) {
		String msg="{\"timestamp\":1383556716,\"rmb\":1,\"paytype\":\"ssss\",\"userid\":\"1504250\",\"appid\":\"460469\",\"quantity\":1,\"virtualcurrency\":10,\"orderid\":\"131454641321321213\",\"deviceId\":\"321321k32k1j3i12\"}";
		System.out.println(getServerPaysucc(msg, ""+System.currentTimeMillis()));
	}

}
