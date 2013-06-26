package com.dp.hcatalog.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

public class SimpleMRExample extends Configured implements Tool {

	public static class Map extends Mapper<WritableComparable, HCatRecord, Text, IntWritable> {
		HCatSchema schema ;
		Text host = new Text();

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			schema = HCatInputFormat.getTableSchema(context);
			if (schema == null) {
				throw new RuntimeException("schema is null");
			}

		}
		@Override
		protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
			// another way, get data by column index
			// host.set(value.get(8));
			host.set(value.getString("host", schema));
			context.write(host, new IntWritable(1));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, WritableComparable, HCatRecord> {
		HCatSchema schema;
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			schema = HCatOutputFormat.getTableSchema(context); 
		}
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				sum++;
				iter.next();
			}
			HCatRecord record = new DefaultHCatRecord(2);
			// another way, set data by column index
			// record.set(0, key.toString());
			// record.set(1, sum);
			record.setString("name", schema, key.toString());
			record.setInteger("count", schema, sum);

			context.write(null, record);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		String inputTable = args[0];
		String filter = "dt>=\""+args[1]+"\" and "+"dt<=\""+args[2]+"\"";
		String outputTable = args[3];
		String outputfilter = args[4];
		int reduceNum = Integer.parseInt(args[5]);
		String dbname = null;

		Job job = new Job(conf, "testMR-groupby-count");
		System.out.println("filter: "+filter);
		HCatInputFormat.setInput(job, InputJobInfo.create(dbname, inputTable, filter));

		job.setInputFormatClass(HCatInputFormat.class);
		job.setJarByClass(SimpleMRExample.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		job.setNumReduceTasks(reduceNum);

		HashMap<String, String> partitions = new HashMap<String, String>(1);
		partitions.put("dt", outputfilter);

		HCatOutputFormat.setOutput(job, OutputJobInfo.create("public", outputTable, partitions));
		HCatSchema s = HCatOutputFormat.getTableSchema(job);

		HCatOutputFormat.setSchema(job, s);
		job.setOutputFormatClass(HCatOutputFormat.class);

		return (job.waitForCompletion(true)?0:1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SimpleMRExample(), args);
		System.exit(exitCode);
	}
}

