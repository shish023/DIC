import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {


	public static void main(String[] args) {

		long start = new Date().getTime();

		Configuration conf = HBaseConfiguration.create();
		try {

			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("mr1"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("xi"));
			if ( admin.isTableAvailable("mr1")) {
				admin.disableTable("mr1");
				admin.deleteTable("mr1");
			}
			admin.createTable(tableDescriptor);


			Job job1 = Job.getInstance();
			job1.setJarByClass(MapReduce1.class);
			job1.setMapperClass(MapReduce1.Map1.class);
			job1.setReducerClass(MapReduce1.Reduce1.class);
			TableMapReduceUtil.initTableReducerJob("mr1", MapReduce1.Reduce1.class, job1);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setNumReduceTasks(5);

			FileInputFormat.addInputPath(job1, new Path(args[0]));

			//**************************************************

			tableDescriptor = new HTableDescriptor(TableName.valueOf("mr2"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("vol"));
			if ( admin.isTableAvailable("mr2")) {
				admin.disableTable("mr2");
				admin.deleteTable("mr2");
			}
			admin.createTable(tableDescriptor);

			Configuration config = HBaseConfiguration.create();
			Job job2 = Job.getInstance(config);
			job2.setJarByClass(MapReduce2.class);     // class that contains mapper and reducer

			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			// set other scan attrs

			TableMapReduceUtil.initTableMapperJob(
			    "mr1",        				// input table
			    scan,               		// Scan instance to control CF and attribute selection
			    MapReduce2.Map2.class,     	// mapper class
			    Text.class,         		// mapper output key
			    Text.class,  				// mapper output value
			    job2);
			TableMapReduceUtil.initTableReducerJob(
			    "mr2",        				// output table
			    MapReduce2.Reduce2.class,	// reducer class
			    job2);

			job2.setNumReduceTasks(5);   	// at least one, adjust as required

			FileOutputFormat.setOutputPath(job2, new Path("temp-mr1"));

			//************************************************
			Job job3 = Job.getInstance(config);
			job3.setJarByClass(MapReduce3.class);     // class that contains mapper and reducer
			job3.setReducerClass(MapReduce3.Reduce3.class);

			job3.setNumReduceTasks(1);

			TableMapReduceUtil.initTableMapperJob(
			    "mr2",        				// input table
			    scan,               		// Scan instance to control CF and attribute selection
			    MapReduce3.Map3.class,     	// mapper class
			    DoubleWritable.class,       // mapper output key
			    Text.class,  				// mapper output value
			    job3);


			FileOutputFormat.setOutputPath(job3, new Path("output"));

			job1.waitForCompletion(true);
			job2.waitForCompletion(true);
			boolean status = job3.waitForCompletion(true);
			if (status == true) {
				long end = new Date().getTime();
				System.out.println("\nJob took " + (end - start) / 1000 + " seconds\n");
			}
			System.out.println("\n*********Hbase_Hadoop-> End**********\n");

			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}




