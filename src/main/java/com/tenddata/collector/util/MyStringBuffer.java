package com.tenddata.collector.util;

public class MyStringBuffer {

	private StringBuffer sb = new StringBuffer();

	private String separator = ",";

	private String replacer = "_";

	public MyStringBuffer append(String v) {
		if (v != null)
			sb.append(v.replace(separator, replacer)).append(separator);
		return this;
	}

	public MyStringBuffer append(int v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(char v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(float v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(long v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(boolean v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(char[] v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(double v) {
		sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(Object v) {
		if (v instanceof java.lang.String)
			append((String) v);
		else
			sb.append(v).append(separator);
		return this;
	}

	public MyStringBuffer append(MyStringBuffer v) {
		sb.append(v.toString());
		return this;
	}

	@Override
	public String toString() {
		return sb.toString();
	}

	// only for this project
	public void appendEnd(String v) {
		if (v != null)
			sb.append(v.replace(separator, replacer));
	}
}
