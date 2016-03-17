package com.tenddata.collector.uuc.control.parse;

import java.util.ArrayList;
import java.util.List;

public class ParsedPackage {

	public List<String> init = new ArrayList<String>();
	public List<String> register = new ArrayList<String>();
	public List<String> login = new ArrayList<String>();
	public List<String> payreq = new ArrayList<String>();
	public List<String> paysucc = new ArrayList<String>();
	public String serverpaysucc = null;

	public String rectime;
}
