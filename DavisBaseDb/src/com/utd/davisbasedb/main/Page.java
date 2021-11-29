package com.utd.davisbasedb.main;

import java.util.Set;

/*
 *  @author Team Rome 
 *  @version 1.0
 *  This class is used to set and get the attributes of the each page
 */

public class Page {

	int pageNo;
	byte pageType;
	Set<Record> pageRecords;
	
	public int getPageNo() {
		return pageNo;
	}
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	public byte getPageType() {
		return pageType;
	}
	public void setPageType(byte pageType) {
		this.pageType = pageType;
	}
	public Set<Record> getPageRecords() {
		return pageRecords;
	}
	public void setPageRecords(Set<Record> pageRecords) {
		this.pageRecords = pageRecords;
	}
}
