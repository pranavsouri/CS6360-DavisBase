package com.utd.davisbasedb.main;



/*
 *  @author Team Rome 
 *  @version 1.0
 *  This class is used to set and get the attributes of the record
 */

public class Record {
	
	

	int pageNo;
	short payLoadLength;
	int rowId;
	short recordLocation;
	byte noOfColumns;
	byte[] dataTypeOfColumn;
	String[] payLoadContent;
	
	public int getPageNo() {
		return pageNo;
	}
	
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	
	public short getPayLoadLength() {
		return payLoadLength;
	}
	
	public void setPayLoadLength(short payLoadLength) {
		this.payLoadLength = payLoadLength;
	}
	
	public int getRowId() {
		return rowId;
	}
	
	public void setRowId(int rowId) {
		this.rowId = rowId;
	}
	
	public short getRecordLocation() {
		return recordLocation;
	}
	
	public void setRecordLocation(short recordLocation) {
		this.recordLocation = recordLocation;
	}
	
	public byte getNoOfColumns() {
		return noOfColumns;
	}
	
	public void setNoOfColumns(byte noOfColumns) {
		this.noOfColumns = noOfColumns;
	}
	
	public byte[] getDataTypeOfColumn() {
		return dataTypeOfColumn;
	}
	
	public void setDataTypeOfColumn(byte[] dataTypeOfColumn) {
		this.dataTypeOfColumn = dataTypeOfColumn;
	}
	
	public String[] getPayLoadContent() {
		return payLoadContent;
	}
	
	public void setPayLoadContent(String[] payLoadContent) {
		this.payLoadContent = payLoadContent;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + rowId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Record other = (Record) obj;
		if (rowId != other.rowId)
			return false;
		return true;
	}
}
