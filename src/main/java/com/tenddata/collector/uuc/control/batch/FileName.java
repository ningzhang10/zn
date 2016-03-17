package com.tenddata.collector.uuc.control.batch;

@SuppressWarnings("unused")
public class FileName {
	/**
	 * event_batchid_size.stage
	 * eg: activity_201307111509_1232.t1over
	 */
	private String event;
	private String batchid; // or date
	private String stage;
	private int size;
	
	public static String t1_uncomplete = ".t1uc";
	public static String t1_over = ".t1";
	public static String t2 = ".t2";
	public static String t3 = ".t3";
	
	public FileName(String filename) {
		event = filename.substring(0, filename.indexOf("_"));
		batchid = filename.substring(filename.indexOf("_") + 1, filename.lastIndexOf("_"));
		size = Integer.parseInt(filename.substring(filename.lastIndexOf("_") + 1, filename.lastIndexOf(".")));
		stage = filename.substring(filename.lastIndexOf("."), filename.length());
	}
	
	public static String getFileName(String event, String batchid, String stage, int size) {
		if (stage.equals(t1_uncomplete)) {
			return event + "_" + batchid + stage;
		}
		return event + "_" + batchid + "_" + size + stage;
	}
	
	public static String rename(String filename, String overStage, String nextStage) {
		return filename.replace(overStage, nextStage);
	}
	
	public static String getNameWithoutSuffix(String filename) {
		return filename.substring(0, filename.lastIndexOf("."));
	}
	
	public static String getSuffix(String filename) {
		return filename.substring(filename.lastIndexOf("."));
	}
	
	public static String t1over(String filename, long size) {
		return getNameWithoutSuffix(filename) + "_" + size + t1_over;
	}
	
	public static String addSize(String filename, int size) {
		return getNameWithoutSuffix(filename) + "_" + size + getSuffix(filename);
	}
	
	public static String getEvent(String filename) {
		return filename.substring(0, filename.indexOf("_"));
	}
	
	public static String getBatchid(String filename) {
		return filename.substring(filename.indexOf("_") + 1, filename.lastIndexOf("_"));
	}
	
	public static int getSize(String filename) {
		return Integer.parseInt(filename.substring(filename.lastIndexOf("_") + 1, filename.lastIndexOf(".")));
	}
	
	public static void main(String[] args) {
		String filename = "/s/d/f/activity_201307111509_1232.t1";
		new FileName(filename);
		System.out.println(FileName.getNameWithoutSuffix(filename));
		System.out.println(FileName.getSuffix(filename));
		System.out.println(FileName.addSize(filename, 555));
	}

}
