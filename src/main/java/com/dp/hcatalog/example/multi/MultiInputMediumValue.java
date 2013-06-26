package com.dp.hcatalog.example.multi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MultiInputMediumValue implements Writable{
	
	private String tableName;
	private int tableIndex;
	private int count;
	
	public MultiInputMediumValue(){}
	
	public MultiInputMediumValue(String tableName, int tableIndex, int count) {
	   super();
	   this.tableName = tableName;
	   this.tableIndex = tableIndex;
	   this.count = count;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getTableIndex() {
		return tableIndex;
	}

	public void setTableIndex(int tableIndex) {
		this.tableIndex = tableIndex;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
   public void readFields(DataInput in) throws IOException {
	   tableName = WritableUtils.readString(in);
	   tableIndex = WritableUtils.readVInt(in);
	   count = WritableUtils.readVInt(in);
   }

	@Override
   public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, tableName);
		WritableUtils.writeVInt(out, tableIndex);
		WritableUtils.writeVInt(out, count);
   }

}
