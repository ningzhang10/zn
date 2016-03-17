package com.tenddata.collector.uuc.control.parse;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.exception.MemcachedException;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import td.idmapping.commons.util.MurmurHash;

import com.tendcloud.tenddata.entity.Activity;
import com.tendcloud.tenddata.entity.AppEvent;
import com.tendcloud.tenddata.entity.AppException;
import com.tendcloud.tenddata.entity.AppProfile;
import com.tendcloud.tenddata.entity.DeviceProfile;
import com.tendcloud.tenddata.entity.EventPackage;
import com.tendcloud.tenddata.entity.Session;
import com.tendcloud.tenddata.entity.TMessage;
import com.tenddata.collector.util.Configuration;
import com.tenddata.collector.util.MemcachBuilder;
import com.tenddata.collector.uuc.control.batch.BatchLogger;
import com.tenddata.kestrel.hadoopbean.HadoopActivity;
import com.tenddata.kestrel.hadoopbean.HadoopAppevent;
import com.tenddata.kestrel.hadoopbean.HadoopDevice;
import com.tenddata.kestrel.hadoopbean.HadoopException;
import com.tenddata.kestrel.hadoopbean.HadoopKeyValue;
import com.tenddata.kestrel.hadoopbean.HadoopLaunch;
import com.tenddata.kestrel.hadoopbean.HadoopTerminate;
import com.tenddata.kestrel.json.KestrelBuilder;

/**
 * <p>Datetime   : 2013-7-1 上午10:42:46</p>
 * <p>Title      : CollectorUtil.java</p>
 * <p>Description: </p>
 * <p>Copyright  : Copyright (c) 2013</p>
 * <p>Company    : TendCloud</p>
 * @author  <a href="mailto:jinhu.fan@tendcloud.com">fjh</a>
 */
public class CollectorUtil {
	
	private static final Log log = LogFactory.getLog("CollectorUtil");
	private static final String  _CARDNUMBER= "_cardnumber";
	private static final String  _SEID= "_SEID";
	private static final String  _ACCOUNT= "_account";
	private static final String  _THIRDACCOUNT= "_thirdaccount";
	private static final String  _PUSHTOKEN= "__tx.push.info";
	public static void logEvent2(EventPackage input, String ip, String rectime) {
		
		Parsed4Package pp = new Parsed4Package();
		pp.rectime = rectime;
		log.info("ip--<" + ip + ">\r\n" + input);		
		
		//生成并向Kestrel发送对象(Redis+Hadoop JSON)		
		for (final TMessage e : input.mTMessages) {
			//mMsgType == 1 Init 事件
			if(e.mMsgType == 1){				
				//向Kestrel-Hadoop-Init中发送消息
				String hadoopJson = getHadoopInit(input,e);
				if(hadoopJson != null){
					pp.device.add(hadoopJson);					
				}
			}
			
			//mMsgType == 2 session事件
			if(e.mMsgType == 2){
				Session session = e.session;
				
				//mStatus == 1 启动事件
				if(session.mStatus == 1){					
					
					String hadoopJson = getHadoopLaunch(input,e,ip);
					if(hadoopJson != null){						
						pp.launch.add(hadoopJson);						
					}				
					
				}
				
				// 循环发送activity事件
				List<Activity> activities = session.activities;
				if (activities != null) {
					for (int act = 0; act < activities.size(); act++) {							
						String hadoopJson = getHadoopActivity(input, e,activities.get(act),ip);
						if (hadoopJson != null) {							
							pp.activity.add(hadoopJson);							
						}
					}
				}
				
				//循环发送App Event
				List<AppEvent> appEvents = session.appEvents;
				if (appEvents != null) {
					for (int eve = 0; eve < appEvents.size(); eve++) {
						
						String event = appEvents.get(eve).id;
						if(event == null){
							continue;
						}
						if (_CARDNUMBER.equals(event)
								||_SEID.equals(event)
								||_ACCOUNT.equals(event)
								||_THIRDACCOUNT.equals(event)) {
							String baseData = EventSpliter.getBaseDate(input, e, ip,appEvents.get(eve));
							pp.thirdID.add(baseData);
						}else if(_PUSHTOKEN.equals(event)){
							putPushToken(input,appEvents.get(eve));							
						}else{
							String hadoopJson = getHadoopAppevent(input,e,appEvents.get(eve),ip);
							if (hadoopJson != null) {
								pp.appevent.add(hadoopJson);							
							}						
							// 向Kestrel-Hadoop-keyvalue中发送消息
							ArrayList<String> hadoopJsonList = getHadoopKeyvalue(input,appEvents.get(eve));
							if (hadoopJsonList != null) {
								for(String keyvalueString : hadoopJsonList){	
									for(int i=0;i<appEvents.get(eve).count;i++){
										pp.keyvalue.add(keyvalueString);		
									}
								}
							}
							
						}
						
					}
				}
				
				//mStatus == 3 Terminate事件
				if(session.mStatus == 3){
					
					String hadoopJson = getHadoopTerminate(input,e);
					if(hadoopJson != null){						
						pp.terminate.add(hadoopJson);						
					}
				}
			}
			//mMsgType == 3 exception事件	
			if(e.mMsgType == 3){
				// e.mAppException 有为null的情况，在此修正 2012-11-02
				if (e.mAppException != null) {
					String hadoopJson = getHadoopException(input,e,e.mAppException,ip);
					if(hadoopJson != null){
						pp.exception.add(hadoopJson);						
					}
				}
				
			}
		}
		BatchLogger.log2(pp);
	}
	public static void logEvent(EventPackage input, String ip, String rectime) {

		log.info(input);
		
		ParsedPackage pp = new ParsedPackage();
		pp.rectime = rectime;
		
		for (final TMessage e : input.mTMessages) {
			// mStatus == 1 Init 事件
			/*
			if (e.mMsgType == 1) {
				String init = EventSpliter.getInit(input, e, ip, rectime);
				pp.init = init;
			}
			*/

			// mStatus == 2 Launch、Continue事件
			if (e.mMsgType == 2) {
				Session session = e.session;

				// mStatus == 1 启动事件
				/*
				if (session.mStatus == 1) {
					String launch = EventSpliter.getLaunch(input, e, ip, rectime);
					pp.launch = launch;
				}
				*/

				// activity事件
				/*
				List<Activity> activities = session.activities;
				if (activities != null) {
					for (Activity activity : activities) {
						String act = EventSpliter.getActivity(input, e, activity, ip, rectime);
						pp.activity.add(act);
					}
				}
				*/

				// App Event
				List<AppEvent> appEvents = session.appEvents;
				if (appEvents != null) {
					for (AppEvent appevent : appEvents) {
						//String appeventstr = EventSpliter.getAppevent(input, e,	appevent, ip, rectime);
						//pp.appevent.add(appeventstr);

						String event = appevent.id;
						Map<String, Object> kvs = appevent.parameters;
						if ("_appStart".equals(event)) {
							String init = EventSpliter.getInit(input, appevent, session, rectime);
							pp.init.add(init);
						} else if ("_uReg".equals(event)) {
							String registe = EventSpliter.getRegiste(input, appevent, kvs, session, rectime);
							pp.register.add(registe);
						} else if ("_uLogin".equals(event)) {
							String login = EventSpliter.getLogin(input, appevent, kvs, session, rectime);
							pp.login.add(login);
						} else if ("_payReq".equals(event)) {
							String payreq = EventSpliter.getPayreq(input, appevent, kvs, session, rectime);
							pp.payreq.add(payreq);
						} else if ("_paySuccess".equals(event)) {
							String paysucc = EventSpliter.getPaysucc(input, appevent, kvs, session, rectime);
							pp.paysucc.add(paysucc);
						}
						
					}
				}

				// mStatus == 3 Terminate事件
				/*
				if (session.mStatus == 3) {
					String terminate = EventSpliter.getTerminate(input, e, ip, rectime);
					pp.terminate = terminate;
				}
				*/
			}

			/*
			if (e.mMsgType == 3) {
				if (e.mAppException != null) {
					String exception = EventSpliter.getException(input, e, e.mAppException, ip, rectime);
					pp.exception = exception;
				}
			}
			*/
		}
		
		// log parsed package
		BatchLogger.log(pp);
	}
	private static String getHadoopInit(EventPackage eventpackage,TMessage tmessage) {
		String retval = "";
		
		HadoopDevice device = new HadoopDevice();
		
		/*
		 * 1.5版本升级，增加以下两字段
		 */
		device.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
		device.setAppStoreID(eventpackage.mAppProfile.appStoreID);
		
		//设置Device信息-设备ID
		device.setDevid(eventpackage.mDeviceId);
				
		//应用启动时间
		device.setApp_starttime(eventpackage.mAppProfile.mStartTime);
		
		//CPU描述信息
		device.setMcpudiscription(tmessage.mInitProfile.mCpuDiscription);
		
		//CPU 核数
		device.setMcpucorenum(tmessage.mInitProfile.mCpuCoreNum);
		
		//CPU 频率
		device.setMcpufrequency(tmessage.mInitProfile.mCpuFrequency);
		
		//CPU 制造商
		device.setMcpuimplementor(tmessage.mInitProfile.mCpuImplementor);
		
		//GPU 渲染芯片
		device.setMgpurenderer(tmessage.mInitProfile.mGpuRenderer);
		
		//GPU 制造商
		device.setMgpuvendor(tmessage.mInitProfile.mGpuVendor);
		
		//内存总容量
		device.setMmemorytotal(tmessage.mInitProfile.mMemoryTotal);
		
		//卡剩余总容量
		device.setMmobilestoragefree(tmessage.mInitProfile.mMobileStorageFree);
		
		//卡剩余总容量
		device.setMsdcardstoragefree(tmessage.mInitProfile.mSDCardStorageFree);
		
		//卡总容量
		device.setMsdcardstoragetotal(tmessage.mInitProfile.mSDCardStorageTotal);
		
		//电池容量
		device.setMbatterycapacity(tmessage.mInitProfile.mBatteryCapacity);
		
		//显示屏宽度
		if (Float.isInfinite(tmessage.mInitProfile.mDisplaMetricWidth) || Float.isNaN(tmessage.mInitProfile.mDisplaMetricWidth)) {
			device.setMdisplametricwidth(0);
		} else {
			device.setMdisplametricwidth(tmessage.mInitProfile.mDisplaMetricWidth);
		}
		
		//显示屏高度
		if (Float.isInfinite(tmessage.mInitProfile.mDisplaMetricHeight) || Float.isNaN(tmessage.mInitProfile.mDisplaMetricHeight)) {
			device.setMdisplametricheight(0);
		} else {
			device.setMdisplametricheight(tmessage.mInitProfile.mDisplaMetricHeight);
		}
		
		//DPI
		device.setMdisplaymetricdensity(tmessage.mInitProfile.mDisplayMetricDensity);
		
		//ROM
		device.setMrominfo(tmessage.mInitProfile.mRomInfo);
		
		//Base band
		device.setMbaseband(tmessage.mInitProfile.mBaseBand);
		
		//IMSI
		device.setMimsi(tmessage.mInitProfile.mIMSI);
		
		//MAC Address
		device.setMmacaddredd(tmessage.mInitProfile.mMACAddress);
		
		//APN Name
		device.setMapnname(tmessage.mInitProfile.mApnName);
		
		//APN MCC
		device.setMapn_mcc(tmessage.mInitProfile.mApn_mcc);
		
		//APN MNC
		device.setMapn_mnc(tmessage.mInitProfile.mApn_mnc);
		
		//APN Proxy
		device.setMapn_proxy(tmessage.mInitProfile.mApn_proxy);
		
		//IMEI
		device.setMimei(tmessage.mInitProfile.mIMEI);
		
		//msimoperator
		device.setMsimoperator(eventpackage.mDeviceProfile.mSimOperator);
		
		//mnetworkoperator
		device.setMnetworkoperator(eventpackage.mDeviceProfile.mNetworkOperator);
		
		//upid
		device.setUpid(tmessage.mInitProfile.mUpid);
		
		//mSimId
		device.setmSimId(tmessage.mInitProfile.mSimId);
		
		//注意：groupid，需要后续处理程序生成
		//需要产品序列号、平台
		device.setProductId(eventpackage.mDeveploperAppkey);
		device.setSequencenumber(eventpackage.mDeveploperAppkey);
		device.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
		
		//retval = KestrelBuilder.bean2string(device);
		retval = device.toHadoop();
				
		return retval;
	}
	private static String getHadoopLaunch(EventPackage eventpackage,TMessage tmessage,String ip) {
		String retval = "";
		
		HadoopLaunch launch = new HadoopLaunch();
		
		/*
		 * 1.5版本升级，增加以下两字段
		 */
		launch.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
		launch.setAppStoreID(eventpackage.mAppProfile.appStoreID);
		
		//设置Device信息-设备ID
		launch.setDevId(eventpackage.mDeviceId);
		
		//设置产品序列号（32字节 String），需要Kestrel读出后，统一处理成Hadoop需要的（Product id+Developer id）
		launch.setProductid(eventpackage.mDeveploperAppkey);
		launch.setSequencenumber(eventpackage.mDeveploperAppkey);
		
		//Session ID
		launch.setSessionId(tmessage.session.id);
		
		//Session 启动时间
		launch.setSession_start(tmessage.session.start);
		
		//间隔时间
		launch.setSession_duration(tmessage.session.duration);
		
		//SDK Version
		launch.setSdkVersion(eventpackage.mAppProfile.mSdkVersion);
		
		//OS 类型
		launch.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
		
		//渠道ID
		launch.setPartnerid(eventpackage.mAppProfile.mPartnerId);
		
		//手机类型
		launch.setMobile(eventpackage.mDeviceProfile.mMobileModel.replaceAll(",", "_"));
		
		//接入类型
		if(tmessage.session.isConnected == -1){
			launch.setChannel("-1");
		}else{
			launch.setChannel(String.valueOf(eventpackage.mDeviceProfile.mChannel));
		}
		
		//OS 版本
		launch.setOsVersion(eventpackage.mDeviceProfile.mOsVersion);
		
		//设置操作系统
		launch.setOs(eventpackage.mDeviceProfile.mOsSdkVersion);
		
		//纬度
		launch.setLat(eventpackage.mDeviceProfile.mGis.lat);
		
		//经度
		launch.setLng(eventpackage.mDeviceProfile.mGis.lng);
		
		//CPU
		//launch.setCpu(eventpackage.mDeviceProfile.mCpuABI);
		
		//分辨率
		launch.setPixel(eventpackage.mDeviceProfile.mPixelMetric);
		
		//国家
		launch.setCountry(eventpackage.mDeviceProfile.mCountry);
		
		//ISP
		launch.setIsp(eventpackage.mDeviceProfile.mCarrier);
		
		//语言
		launch.setLanguage(eventpackage.mDeviceProfile.mLanguage);
		
		//时区
		launch.setTimezone(eventpackage.mDeviceProfile.mTimezone);
		
		//应用版本
		//launch.setApp_version(eventpackage.mAppProfile.mAppVersionCode);
		launch.setVersionCode(eventpackage.mAppProfile.mAppVersionCode);
		launch.setVersionName(eventpackage.mAppProfile.mAppVersionName);
		
		// 对应2012-08-09 kestrel-bean 的变更
		// APN MCC
		//launch.setMapn_mcc(tmessage.mInitProfile.mApn_mcc);
		// APN MNC
		//launch.setMapn_mnc(tmessage.mInitProfile.mApn_mnc);
		// 对应2013-02-19 变更
		String networkOperator = eventpackage.mDeviceProfile.mNetworkOperator;
		String simOperator = eventpackage.mDeviceProfile.mSimOperator;
		if (!StringUtils.isEmpty(networkOperator) && networkOperator.length() >= 5) {
			String mccmnc = networkOperator.split(",")[0];
			launch.setMapn_mcc(mccmnc.substring(0, 3));	// mcc 前三位
			launch.setMapn_mnc(mccmnc.substring(3));	// mnc 三位以后
		} else if (!StringUtils.isEmpty(simOperator) && simOperator.length() >= 5) {
			String mccmnc = simOperator.split(",")[0];
			launch.setMapn_mcc(mccmnc.substring(0, 3));	// mcc 前三位
			launch.setMapn_mnc(mccmnc.substring(3));	// mnc 三位以后
		}
		
		launch.setIp(ip);
		
		//应用名
		launch.setApp_name(eventpackage.mAppProfile.mAppPackageName);
		
		//IP
		launch.setIp(ip);
		
		//是否越狱
		if(eventpackage.mDeviceProfile.isJailBroken == true)
			launch.setJailbroken(1);
		else
			launch.setJailbroken(0);
		
		//是否盗版
		if(eventpackage.mAppProfile.isCracked == true)
			launch.setCracked(1);
		else
			launch.setCracked(0);
		
		//hostname
		//launch.setHostname(eventpackage.mDeviceProfile.hostName);
		
		//devicename
		//launch.setDevicename(eventpackage.mDeviceProfile.deviceName);
		
		//kernboottime
		//launch.setKernBootTime(eventpackage.mDeviceProfile.kernBootTime);
		
		//installationtime
		//launch.setInstallationTime(eventpackage.mAppProfile.installationTime);
		
		//purchasetime
		//launch.setPurchaseTime(eventpackage.mAppProfile.purchaseTime);
		
		//retval = KestrelBuilder.bean2string(launch);
		retval = launch.toHadoop();
		
		return retval;
	}
	
	/**
	 * 
	 * 拼接用于Hadoop Terminate Bean的字符串，返回Json格式字符串
	 * 
	 * devId:string             productId:string
	 * sessionId:string         session_start:long
	 * session_duration:int     usetime_level:int
	 * flatformid:int           app_version:string
	 * partnerid:string         developerid:int
	 * 
	 * @param eventpackage
	 * @param tmessage
	 * @return
	 */
	private static String getHadoopTerminate(EventPackage eventpackage,TMessage tmessage) {
		String retval = "";
		
		HadoopTerminate terminate = new HadoopTerminate();
		
		/*
		 * 1.5版本升级，增加以下两字段
		 */
		terminate.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
		terminate.setAppStoreID(eventpackage.mAppProfile.appStoreID);
		
		//设置Device信息-设备ID
		terminate.setDevId(eventpackage.mDeviceId);
		
		//设置产品序列号（32字节 String），需要Kestrel读出后，统一处理成Hadoop需要的（Product id+Developer id）
		terminate.setProductid(eventpackage.mDeveploperAppkey);
		terminate.setSequencenumber(eventpackage.mDeveploperAppkey);
		
		//Session ID
		terminate.setSessionId(tmessage.session.id);
		
		//Session 启动时间
		terminate.setSession_start(tmessage.session.start);
		
		//间隔时间
		terminate.setSession_duration(Long.valueOf(tmessage.session.duration));
		
		//OS 类型
		terminate.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
		
		//App version
		//terminate.setApp_version(eventpackage.mAppProfile.mAppVersionCode);
		terminate.setVersionCode(eventpackage.mAppProfile.mAppVersionCode);
		terminate.setVersionName(eventpackage.mAppProfile.mAppVersionName);
		terminate.setSdkVersion(eventpackage.mAppProfile.mSdkVersion);
		
		//渠道ID
		terminate.setPartnerid(eventpackage.mAppProfile.mPartnerId);
				
		//retval = KestrelBuilder.bean2string(terminate);
		retval = terminate.toHadoop();
		
		return retval;
	}
	
	/**
	 * 
	 * 拼接用于Hadoop Activity Bean的字符串，返回Json格式字符串
	 * 
	 * devId:string             productId:string
	 * sessionId:string         starttime:long
	 * duration:int             app_version:string
	 * refpagename:string       pagename:string
	 * flatformid:int           partnerid:string
	 * developerid:int
	 * 
	 * @param eventpackage
	 * @param tmessage
	 * @param activity
	 * @return
	 */
	private static String getHadoopActivity(EventPackage eventpackage,TMessage tmessage,Activity activity,String ip) {
		String retval = "";

		HadoopActivity hadoopactivity = new HadoopActivity();
		
		/*
		 * 1.5版本升级，增加以下两字段
		 */
		hadoopactivity.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
		hadoopactivity.setAppStoreID(eventpackage.mAppProfile.appStoreID);
		
		//设置Device信息-设备ID
		hadoopactivity.setDevId(eventpackage.mDeviceId);

		// 设置产品序列号（32字节 String），需要Kestrel读出后，统一处理成Hadoop需要的（Product id+Developer id）
		hadoopactivity.setProductid(eventpackage.mDeveploperAppkey);
		hadoopactivity.setSequencenumber(eventpackage.mDeveploperAppkey);

		// Session ID
		hadoopactivity.setSessionId(tmessage.session.id);
		
		// 开始时间
		hadoopactivity.setStarttime(activity.start);
		
		// 持续时间
		hadoopactivity.setDuration(activity.duration);
		
		// App version
		//hadoopactivity.setApp_version(eventpackage.mAppProfile.mAppVersionCode);
		hadoopactivity.setVersionCode(eventpackage.mAppProfile.mAppVersionCode);
		hadoopactivity.setVersionName(eventpackage.mAppProfile.mAppVersionName);
		hadoopactivity.setSdkVersion(eventpackage.mAppProfile.mSdkVersion);
		
		//引用页面
		hadoopactivity.setRefpagename(activity.refer);
		
		//指向页面
		hadoopactivity.setPagename(activity.name);

		// OS 类型
		hadoopactivity.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));

		// 渠道ID
		hadoopactivity.setPartnerid(eventpackage.mAppProfile.mPartnerId);
		
		//是否盗版
		if(eventpackage.mAppProfile.isCracked == true)
			hadoopactivity.setCracked(1);
		else
			hadoopactivity.setCracked(0);
		//手机类型
		hadoopactivity.setMobile(eventpackage.mDeviceProfile.mMobileModel);
		//分辨率
		hadoopactivity.setPixel(eventpackage.mDeviceProfile.mPixelMetric);
		//OS 版本
		hadoopactivity.setOsVersion(eventpackage.mDeviceProfile.mOsVersion);
		//设置操作系统
		hadoopactivity.setOs(eventpackage.mDeviceProfile.mOsSdkVersion);
		String networkOperator = eventpackage.mDeviceProfile.mNetworkOperator;
		String simOperator = eventpackage.mDeviceProfile.mSimOperator;
		if (!StringUtils.isEmpty(networkOperator) && networkOperator.length() >= 5) {
			String mccmnc = networkOperator.split(",")[0];
			hadoopactivity.setMapn_mcc(mccmnc.substring(0, 3));	// mcc 前三位
			hadoopactivity.setMapn_mnc(mccmnc.substring(3));	// mnc 三位以后
		} else if (!StringUtils.isEmpty(simOperator) && simOperator.length() >= 5) {
			String mccmnc = simOperator.split(",")[0];
			hadoopactivity.setMapn_mcc(mccmnc.substring(0, 3));	// mcc 前三位
			hadoopactivity.setMapn_mnc(mccmnc.substring(3));	// mnc 三位以后
		}
		//ISP
		hadoopactivity.setIsp(eventpackage.mDeviceProfile.mCarrier);
		
		//语言
		hadoopactivity.setLanguage(eventpackage.mDeviceProfile.mLanguage);
		hadoopactivity.setIp(ip);
		//国家
		hadoopactivity.setCountry(eventpackage.mDeviceProfile.mCountry);
		//接入类型
		if(tmessage.session.isConnected == -1){
			hadoopactivity.setChannel("-1");
		}else{
			hadoopactivity.setChannel(String.valueOf(eventpackage.mDeviceProfile.mChannel));
		}
		//是否越狱
		if(eventpackage.mDeviceProfile.isJailBroken == true)
			hadoopactivity.setJailbroken(1);
		else
			hadoopactivity.setJailbroken(0);
		
		//retval = KestrelBuilder.bean2string(hadoopactivity);
		retval = hadoopactivity.toHadoop();
		
		return retval;
	}
	
	/**
	 * 
	 * 拼接用于Hadoop Appevent Bean的字符串，返回Json格式字符串
	 * 
	 * devId:string           productId:string
	 * app_version:string     eventId:string
	 * label:string           start:long
	 * eventcount:int         platformid:int
	 * partnerid:string       developerid:int
	 * 
	 * @param eventpackage
	 * @param tmessage
	 * @param activity
	 * @return
	 */
	private static String getHadoopAppevent(EventPackage eventpackage,TMessage tmessage,AppEvent appevent,String ip) {
		String retval = "";

		HadoopAppevent hadoopappevent = new HadoopAppevent();
		
		/*
		 * 1.5版本升级，增加以下两字段
		 */
		hadoopappevent.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
		hadoopappevent.setAppStoreID(eventpackage.mAppProfile.appStoreID);
		
		//设置Device信息-设备ID
		hadoopappevent.setDevId(eventpackage.mDeviceId);

		// 设置产品序列号（32字节 String），需要Kestrel读出后，统一处理成Hadoop需要的（Product id+Developer id）
		hadoopappevent.setProductid(eventpackage.mDeveploperAppkey);
		hadoopappevent.setSequencenumber(eventpackage.mDeveploperAppkey);
		
		// App version
		//hadoopappevent.setApp_version(eventpackage.mAppProfile.mAppVersionCode);
		hadoopappevent.setVersionCode(eventpackage.mAppProfile.mAppVersionCode);
		hadoopappevent.setVersionName(eventpackage.mAppProfile.mAppVersionName);
		hadoopappevent.setSdkVersion(eventpackage.mAppProfile.mSdkVersion);
		
		// Partnerid
		hadoopappevent.setPartnerid(eventpackage.mAppProfile.mPartnerId);

		// OS 类型
		hadoopappevent.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
		
		// 事件触发次数
		hadoopappevent.setEventcount(appevent.count);
		
		// 事件ID
		hadoopappevent.setEventId(appevent.id);
		
		// 事件标签
		hadoopappevent.setLabel(appevent.label);
		
		//设置Event的发生时间
		hadoopappevent.setStart(appevent.startTime);
		
		//是否盗版
		if(eventpackage.mAppProfile.isCracked == true)
			hadoopappevent.setCracked(1);
		else
			hadoopappevent.setCracked(0);
		//手机类型
		hadoopappevent.setMobile(eventpackage.mDeviceProfile.mMobileModel.replaceAll(",", "_"));
		//分辨率
		hadoopappevent.setPixel(eventpackage.mDeviceProfile.mPixelMetric);
		//OS 版本
		hadoopappevent.setOsVersion(eventpackage.mDeviceProfile.mOsVersion);
		//设置操作系统
		hadoopappevent.setOs(eventpackage.mDeviceProfile.mOsSdkVersion);
		String networkOperator = eventpackage.mDeviceProfile.mNetworkOperator;
		String simOperator = eventpackage.mDeviceProfile.mSimOperator;
		if (!StringUtils.isEmpty(networkOperator) && networkOperator.length() >= 5) {
			String mccmnc = networkOperator.split(",")[0];
			hadoopappevent.setMapn_mcc(mccmnc.substring(0, 3));	// mcc 前三位
			hadoopappevent.setMapn_mnc(mccmnc.substring(3));	// mnc 三位以后
		} else if (!StringUtils.isEmpty(simOperator) && simOperator.length() >= 5) {
			String mccmnc = simOperator.split(",")[0];
			hadoopappevent.setMapn_mcc(mccmnc.substring(0, 3));	// mcc 前三位
			hadoopappevent.setMapn_mnc(mccmnc.substring(3));	// mnc 三位以后
		}
		//ISP
		hadoopappevent.setIsp(eventpackage.mDeviceProfile.mCarrier);
		
		//语言
		hadoopappevent.setLanguage(eventpackage.mDeviceProfile.mLanguage);
		hadoopappevent.setIp(ip);
		//国家
		hadoopappevent.setCountry(eventpackage.mDeviceProfile.mCountry);
		//接入类型
		if(tmessage.session.isConnected == -1){
			hadoopappevent.setChannel("-1");
		}else{
			hadoopappevent.setChannel(String.valueOf(eventpackage.mDeviceProfile.mChannel));
		}
		//是否越狱
		if(eventpackage.mDeviceProfile.isJailBroken == true)
			hadoopappevent.setJailbroken(1);
		else
			hadoopappevent.setJailbroken(0);
		
		//retval = KestrelBuilder.bean2string(hadoopappevent);
		retval = hadoopappevent.toHadoop();
		
		return retval;
	}
	
	/**
	 * 
	 * 拼接用于Hadoop Keyvalue Bean的字符串，返回Json格式字符串，每一个Key值返回一行数据
	 * 
	 * devId:string           productId:string
	 * app_version:string     eventId:string
	 * label:string           start:long
	 * eventcount:int         platformid:int
	 * partnerid:string       developerid:int
	 * key:string             value:string
	 * type:string
	 * 
	 * @param eventpackage
	 * @param tmessage
	 * @param activity
	 * @return
	 */
	private static ArrayList<String> getHadoopKeyvalue(EventPackage eventpackage,AppEvent appevent) {
		ArrayList<String> retval = new ArrayList<String>();
		
		Map<String, Object> keyvalue = appevent.parameters;
		if(keyvalue == null){
			return null;
		}

		//设置Key Value数据
		//System.out.println("上传Key Value:[" + keyvalue.size() + "]");
		Iterator iter = keyvalue.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			Object val = entry.getValue();
			
			HadoopKeyValue hadoopKeyValue = new HadoopKeyValue();
			
			/*
			 * 1.5版本升级，增加以下两字段
			 */
			hadoopKeyValue.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
			hadoopKeyValue.setAppStoreID(eventpackage.mAppProfile.appStoreID);
			
			//设置Device信息-设备ID
			hadoopKeyValue.setDevId(eventpackage.mDeviceId);

			// 设置产品序列号（32字节 String），需要Kestrel读出后，统一处理成Hadoop需要的（Product id+Developer id）
			hadoopKeyValue.setProductid(eventpackage.mDeveploperAppkey);
			hadoopKeyValue.setSequencenumber(eventpackage.mDeveploperAppkey);
			
			// App version
			//hadoopKeyValue.setApp_version(eventpackage.mAppProfile.mAppVersionCode);
			hadoopKeyValue.setVersionCode(eventpackage.mAppProfile.mAppVersionCode);
			hadoopKeyValue.setVersionName(eventpackage.mAppProfile.mAppVersionName);
			hadoopKeyValue.setSdkVersion(eventpackage.mAppProfile.mSdkVersion);

			// Partnerid
			hadoopKeyValue.setPartnerid(eventpackage.mAppProfile.mPartnerId);
						
			// OS 类型
			hadoopKeyValue.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
			
			// 事件触发次数
			hadoopKeyValue.setEventcount(appevent.count);
			
			// 事件ID
			hadoopKeyValue.setEventId(appevent.id);
			
			// 事件标签
			hadoopKeyValue.setLabel(appevent.label);
			
			//设置Event的发生时间
			hadoopKeyValue.setStart(appevent.startTime);
			
			if (val instanceof String) {
				hadoopKeyValue.setKey(""+key);
				hadoopKeyValue.setValue(""+val);
				hadoopKeyValue.setType(HadoopKeyValue.VALUE_TYPE_STRING);
				//retval.add(KestrelBuilder.bean2string(hadoopKeyValue));
				retval.add(hadoopKeyValue.toHadoop());
			} else if (val instanceof Double) {
				hadoopKeyValue.setKey(""+key);
				hadoopKeyValue.setValue(""+val);
				hadoopKeyValue.setType(HadoopKeyValue.VALUE_TYPE_NUMBER);				
				//retval.add(KestrelBuilder.bean2string(hadoopKeyValue));
				retval.add(hadoopKeyValue.toHadoop());
			} else {
				continue;
			}
		}
		
		return retval;
	}
	
	/**
	 * 
	 * 拼接用于Hadoop Exception Bean的字符串，返回Json格式字符串
	 * 
	 * productId:string         app_version:string
	 * osVersion:string         mobile:string
	 * time:long                errorName:string
	 * errorMessage:string      devId:string
	 * hashcode:string          tree_hashcode:string
	 * errcount:int             platformid:int
	 * developerid:int
	 * 
	 * @param eventpackage
	 * @param appException
	 * @return
	 */
	private static String getHadoopException(EventPackage eventpackage,TMessage tmessage, AppException appException,String ip) {
		String retval = "";
		
		HadoopException  exceptiondata = new HadoopException ();
		
		/*
		 * 1.5版本升级，增加以下两字段
		 */
		exceptiondata.setAdvertisingID(eventpackage.mDeviceProfile.advertisingID);
		exceptiondata.setAppStoreID(eventpackage.mAppProfile.appStoreID);
		
		//设置ProductID
		exceptiondata.setProductid(eventpackage.mDeveploperAppkey);
		exceptiondata.setSequencenumber(eventpackage.mDeveploperAppkey);
		
		//设置产品版本
		//exceptiondata.setApp_version(eventpackage.mAppProfile.mAppVersionCode);
		exceptiondata.setVersionCode(eventpackage.mAppProfile.mAppVersionCode);
		exceptiondata.setVersionName(eventpackage.mAppProfile.mAppVersionName);
		exceptiondata.setSdkVersion(eventpackage.mAppProfile.mSdkVersion);
		
		//设置操作系统版本（Android或者IOS），需要Kestrel读出后，统一从字符串处理成ID（Android:1，IOS:2等）
		exceptiondata.setOsVersion(eventpackage.mDeviceProfile.mOsVersion);
		
		//设置机型
		exceptiondata.setMobile(eventpackage.mDeviceProfile.mMobileModel.replaceAll(",", "_"));
		
		//设置Device信息-设备ID
		exceptiondata.setDevId(eventpackage.mDeviceId);
		
		//设置操作系统版本
		exceptiondata.setOsVersion(eventpackage.mDeviceProfile.mOsVersion);
		
		//设置操作系统
		exceptiondata.setOs(eventpackage.mDeviceProfile.mOsSdkVersion);
		
		//设置错误名称
		String errorname = getExceptionName(appException.data);
		if(errorname == null)
			return null;
		else{
				String en;
				try {
					en = URLEncoder.encode(errorname,"UTF-8");
					exceptiondata.setErrorName(en);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
					return null;
				}
			
			}
		
		//设置错误信息
		String errormessage = getExceptionMessage(appException.data);
		if(errormessage == null)
			return null;
		else{
			try {
				String em = URLEncoder.encode(errormessage,"UTF-8");
				exceptiondata.setErrorMessage(em);
			} catch (UnsupportedEncodingException e) {				
				e.printStackTrace();
				return null;
			}
			
		}
		
		//设置错误发生次数
		exceptiondata.setErrcount(appException.mRepeat);
		
		//设置错误发生时间
		exceptiondata.setTime(appException.mErrorTime);
		
		// OS 类型
		exceptiondata.setPlatformid(getPlatform(eventpackage.mDeviceProfile,eventpackage.mAppProfile));

		//设置Hash
		if((appException.mShortHashCode == null)||(appException.mShortHashCode.trim().length()!=32)){
			exceptiondata.setHashcode(""+errorname.trim().hashCode());
		}else{
			exceptiondata.setHashcode(""+appException.mShortHashCode.hashCode());
		}

		// 设置Hash
		exceptiondata.setTree_hashcode("" + (errorname.trim() + errormessage.trim()).hashCode());
		
		//是否盗版
		if(eventpackage.mAppProfile.isCracked == true)
			exceptiondata.setCracked(1);
		else
			exceptiondata.setCracked(0);
		//手机类型
		exceptiondata.setMobile(eventpackage.mDeviceProfile.mMobileModel.replaceAll(",", "_"));
		//分辨率
		exceptiondata.setPixel(eventpackage.mDeviceProfile.mPixelMetric);
		
		String networkOperator = eventpackage.mDeviceProfile.mNetworkOperator;
		String simOperator = eventpackage.mDeviceProfile.mSimOperator;
		if (!StringUtils.isEmpty(networkOperator) && networkOperator.length() >= 5) {
			exceptiondata.setMapn_mcc(networkOperator.substring(0, 3));	// mcc 前三位
			exceptiondata.setMapn_mnc(networkOperator.substring(3));	// mnc 三位以后
		} else if (!StringUtils.isEmpty(simOperator) && simOperator.length() >= 5) {
			exceptiondata.setMapn_mcc(simOperator.substring(0, 3));	// mcc 前三位
			exceptiondata.setMapn_mnc(simOperator.substring(3));	// mnc 三位以后
		}
		//ISP
		exceptiondata.setIsp(eventpackage.mDeviceProfile.mCarrier);
		
		//语言
		exceptiondata.setLanguage(eventpackage.mDeviceProfile.mLanguage);
		exceptiondata.setIp(ip);
		//国家
		exceptiondata.setCountry(eventpackage.mDeviceProfile.mCountry);
		//接入类型
		exceptiondata.setChannel(String.valueOf(eventpackage.mDeviceProfile.mChannel));
		
		//是否越狱
		if(eventpackage.mDeviceProfile.isJailBroken == true)
			exceptiondata.setJailbroken(1);
		else
			exceptiondata.setJailbroken(0);
		//渠道ID
		exceptiondata.setPartnerid(eventpackage.mAppProfile.mPartnerId);
		
		//retval = KestrelBuilder.bean2string(exceptiondata);
		retval = exceptiondata.toHadoop();
		return retval;
	}	
	private static void putPushToken(EventPackage eventpackage,AppEvent appevent) {
	
		Map<String, Object> keyvalue = appevent.parameters;
		Map<String,Object> map = new HashMap<String, Object>();
		if(keyvalue != null){
			Map<String,Object> valMap = new HashMap<String, Object>();
			Map<String,Object> dtMap = new HashMap<String,Object>();
			String[] dtArr = keyvalue.get("deviceToken").toString().replaceAll("[{}\"]", "").split(":");
			dtMap.put(dtArr[0], dtArr[1]);
			valMap.put("deviceToken",dtMap);
			valMap.put("channel", keyvalue.get("channel"));
			valMap.put("appId", keyvalue.get("appId"));
			map.put("id", "G16");
			map.put("deviceId", MurmurHash.hash64(eventpackage.mDeviceId));
			map.put("platformId", getPlatformCode(eventpackage.mDeviceProfile,eventpackage.mAppProfile));
			map.put("val", valMap);
			JSONObject json = JSONObject.fromObject( map ); 
			try {
					MemcachBuilder.getInstance().getClient().set(Configuration.get("kestrel.push"), 0, json.toString());
				} catch (TimeoutException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (MemcachedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}		
				
	}
	/**
	 * 
	 * 从DeviceProfile和AppProfile中拼接给后方的Platform信息
	 * 
	 * @param devProfile
	 * @param appProfile
	 * @return
	 */
	public static String getPlatform(DeviceProfile devProfile , AppProfile appProfile){
		String retval = "";
		
		String mSdkVersion = appProfile.mSdkVersion.toLowerCase();
		if(mSdkVersion.indexOf("android")!=-1){
			retval += "Android+";
			retval += devProfile.mOsVersion;
		}
		
		if(mSdkVersion.indexOf("ios")!=-1){
			retval += "IOS+1";
		}
		
		if(mSdkVersion.indexOf("wphone")!=-1){
			retval += "wphone";
		}
		
		return retval;
	}
	
	/**
	 * 对应平台code
	 * 1: android
	 * 2: ios
	 * 4: wphone
	 * 0: error
	 */
	private static String getPlatformCode(DeviceProfile devProfile , AppProfile appProfile){
		String retval = "0";
		
		String mSdkVersion = appProfile.mSdkVersion.toLowerCase();
		if(mSdkVersion.indexOf("android")!=-1){
			retval = "1";
		}
		
		if(mSdkVersion.indexOf("ios")!=-1){
			retval = "2";
		}
		
		if(mSdkVersion.indexOf("wphone")!=-1){
			retval = "4";
		}
		
		return retval;
	}
	/**
	 * 
	 * 从SDK上传的byte[] 数据中获得"Exception Name"
	 * 
	 * @param data
	 * @return
	 */
	private static String getExceptionName(byte[] data) {
		if(data == null)
			return null;
		
		String errorinfo = new String(data);
		String[] errorinfos = errorinfo.split("\r\n");
		String errorname = "";
		if (errorinfos != null && errorinfos.length > 0) {
			errorname=errorinfos[0];
			return errorname.trim();
		}else
			return null;
	}

	/**
	 * 
	 * 将SDK上传的byte[] 数据，格式化成String<br>
	 * 1、将"\r\n"转化成"#$#"<br>
	 * 2、将","转化成"#~#"<br>
	 * 
	 * @param data
	 * @return
	 */
	private static String getExceptionMessage(byte[] data) {
		if(data == null)
			return null;
		
		String errorinfo = new String(data);
		String[] errorinfos = errorinfo.split("\r\n");
		String errormessage = "";
		if (errorinfos != null && errorinfos.length > 0) {
			for (int i = 1; i < errorinfos.length; i++) {
				if (i != errorinfos.length - 1) {
					errormessage = errormessage + errorinfos[i] + "#$#";
				} else {
					errormessage = errormessage + errorinfos[i];
				}
			}
			errormessage = errormessage.replaceAll(",", "#~#");
			return errormessage.trim();
		}else{
			return null;
		}	
	}
	
}
