package com.dp.hcatalog.example.multi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatMultiInputFormat;
import org.apache.hcatalog.mapreduce.HCatMultiSplit;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.MultiOutputFormat;
import org.apache.hcatalog.mapreduce.MultiOutputFormat.JobConfigurer;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

public class MultiInputOutputMRExample extends Configured implements Tool{

	private static final String OUTPUT1_TABLE_NAME = "hostcount1";
	private static final String OUTPUT2_TABLE_NAME = "hostcount2";
	private static final String OUTPUT_DB_NAME = "tmp";

	public static class MultiMap extends Mapper<WritableComparable, HCatRecord, Text, MultiInputMediumValue> {

		HCatSchema schema;
		String tableName;
		int tableIndex;
		Map<String, Set<Integer>> hostToCitySetMap;
		Map<String, Integer> hostToPVMap;

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			HCatMultiSplit split = (HCatMultiSplit)context.getInputSplit();
			tableName = split.getTableName();
			tableIndex = split.getTableIndex();
			schema = HCatMultiInputFormat.getTableSchema(context, tableName);
			if(schema == null){
				throw new RuntimeException("schema is empty");
			}
			hostToCitySetMap = new HashMap<String, Set<Integer>>();
			hostToPVMap = new HashMap<String, Integer>();
		}
		@Override
		protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
			if(tableName.equals("default.hippolog")){
				if(tableIndex == 1){
					//calculate pv by host
					String host = value.getString("host", schema);
					if(host == null){
						host = "";
					}
					Integer pvObj = hostToPVMap.get(host);
					if(pvObj == null){
						pvObj = 0;
					}
					int pv = pvObj.intValue() + 1;
					hostToPVMap.put(host, pv);
					//MultiInputMediumValue count = new MultiInputMediumValue(tableName, tableIndex, 1);
					//context.write(new Text(host), count);
				}else if(tableIndex == 2){
					//calculate city count by host
					String host = value.getString("host", schema);
					if(host == null){
						host = "";
					}
					Integer city = value.getInteger("city", schema);
					Set<Integer> citySet = hostToCitySetMap.get(host);
					if(citySet == null){
						citySet = new HashSet<Integer>();
						hostToCitySetMap.put(host, citySet);
					}
					citySet.add(city);
				}
			}
		}
		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) 
				throws IOException, InterruptedException{
			if(!hostToCitySetMap.isEmpty()){
				for(String host : hostToCitySetMap.keySet()){
					Set<Integer> citySet = hostToCitySetMap.get(host);
					MultiInputMediumValue count = new MultiInputMediumValue(tableName, tableIndex, citySet.size());
					context.write(new Text(host), count);
				}
			}
			if(!hostToPVMap.isEmpty()){
				for(String host : hostToPVMap.keySet()){
					int pv = hostToPVMap.get(host);
					MultiInputMediumValue count = new MultiInputMediumValue(tableName, tableIndex, pv);
					context.write(new Text(host), count);
				}
			}
		}
	}

	public static class MultiReduce extends Reducer<Text, MultiInputMediumValue, WritableComparable, HCatRecord> {

		HCatSchema hostCount1Schema;
		HCatSchema hostCount2Schema;

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			JobContext jobContext = MultiOutputFormat.getJobContext(OUTPUT1_TABLE_NAME, context);
			hostCount1Schema = HCatOutputFormat.getTableSchema(jobContext);
			if(hostCount1Schema == null){
				throw new RuntimeException("The schema of table " + OUTPUT1_TABLE_NAME + " is empty!");
			}

			jobContext = MultiOutputFormat.getJobContext(OUTPUT2_TABLE_NAME, context);
			hostCount2Schema = HCatOutputFormat.getTableSchema(jobContext);
			if(hostCount2Schema == null){
				throw new RuntimeException("The schema of table " + OUTPUT2_TABLE_NAME + " is empty!");
			}
		}
		@Override
		protected void reduce(Text key, Iterable<MultiInputMediumValue> values, Context context) throws IOException, InterruptedException {
			int pv = 0; 
			int cityCount = 0;
			for(MultiInputMediumValue value : values){
				if(value.getTableIndex() == 1){
					pv += value.getCount();
				}
				else if(value.getTableIndex() == 2){
					cityCount += value.getCount(); 
				}
			}

			//write to output table 1
			HCatRecord record1 = new DefaultHCatRecord(3);
			record1.setString("name", hostCount1Schema, key.toString());
			record1.setLong("pv", hostCount1Schema, (long)pv);
			record1.setInteger("citycount", hostCount1Schema, cityCount);
			MultiOutputFormat.write(OUTPUT1_TABLE_NAME, null, record1, context);

			//write to output table 2
			HCatRecord record2 = new DefaultHCatRecord(3);
			record2.setString("name", hostCount1Schema, key.toString());
			record2.setLong("pv", hostCount1Schema, (long)pv);
			record2.setInteger("citycount", hostCount1Schema, cityCount);
			MultiOutputFormat.write(OUTPUT2_TABLE_NAME, null, record2, context);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		String dbName = "default";
		String tableName = "hippolog";
		String filter = "dt=\"2013-06-25\"";
		int reduceNum = 5;

		//Set InputFormat
		Job job = new Job(conf, "Test Multiple Input Output");
		ArrayList<InputJobInfo> inputJobInfoList = new ArrayList<InputJobInfo>();
		inputJobInfoList.add(InputJobInfo.create(dbName, tableName, filter));
		inputJobInfoList.add(InputJobInfo.create(dbName, tableName, filter));
		HCatMultiInputFormat.setInput(job, inputJobInfoList);
		job.setInputFormatClass(HCatMultiInputFormat.class);

		job.setJarByClass(MultiInputOutputMRExample.class);
		job.setMapperClass(MultiMap.class);
		job.setReducerClass(MultiReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MultiInputMediumValue.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		job.setNumReduceTasks(reduceNum);

		//Set OutputFormat
		JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);
		HashMap<String, String> partitionValues = new HashMap<String, String>();
		partitionValues.put("dt", "2013-06-25");
		//Set output table 1
		configurer.addOutputFormat(OUTPUT1_TABLE_NAME, HCatOutputFormat.class, 
				BytesWritable.class, HCatRecord.class);
		HCatOutputFormat.setOutput(configurer.getJob(OUTPUT1_TABLE_NAME), 
				OutputJobInfo.create(OUTPUT_DB_NAME, OUTPUT1_TABLE_NAME, partitionValues));
		HCatOutputFormat.setSchema(configurer.getJob(OUTPUT1_TABLE_NAME), 
				HCatOutputFormat.getTableSchema(configurer.getJob(OUTPUT1_TABLE_NAME)));
		//Set output table 2
		configurer.addOutputFormat(OUTPUT2_TABLE_NAME, HCatOutputFormat.class, 
				BytesWritable.class, HCatRecord.class);
		HCatOutputFormat.setOutput(configurer.getJob(OUTPUT2_TABLE_NAME), 
				OutputJobInfo.create(OUTPUT_DB_NAME, OUTPUT2_TABLE_NAME, partitionValues));
		HCatOutputFormat.setSchema(configurer.getJob(OUTPUT2_TABLE_NAME), 
				HCatOutputFormat.getTableSchema(configurer.getJob(OUTPUT2_TABLE_NAME)));
		configurer.configure();
		job.setOutputFormatClass(MultiOutputFormat.class);

		return (job.waitForCompletion(true)?0:1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MultiInputOutputMRExample(), args);
		System.exit(exitCode);
	}
}
