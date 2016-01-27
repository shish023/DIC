import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		Configuration conf = new Configuration();
		
	     Job job1 = Job.getInstance();
	     job1.setJarByClass(MapReduce1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(MapReduce2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(MapReduce3.class);
		 

		System.out.println("\n**********Stocks -> Start**********\n");

		job1.setJarByClass(MapReduce1.class);
		job1.setMapperClass(MapReduce1.Map1.class);
		job1.setReducerClass(MapReduce1.Reduce1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
//		job.setNumReduceTasks(5);// decide how many output file
		int NOfReducer1 = Integer.valueOf(args[1]);	
		job1.setNumReduceTasks(NOfReducer1);
	
//		job.setPartitionerClass(MapReduce1.CustomPartitioner.class);

		job2.setJarByClass(MapReduce2.class);
		job2.setMapperClass(MapReduce2.Map2.class);
		job2.setReducerClass(MapReduce2.Reduce2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
//		job2.setNumReduceTasks(5);
		int NOfReducer2 = Integer.valueOf(args[1]);
		job2.setNumReduceTasks(NOfReducer2);
		
		job3.setJarByClass(MapReduce3.class);
		job3.setMapperClass(MapReduce3.Map3.class);
		job3.setReducerClass(MapReduce3.Reduce3.class);

		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
//		job2.setNumReduceTasks(5);
		int NOfReducer3 = 1;
		job3.setNumReduceTasks(NOfReducer3);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp-mr1"));
		FileInputFormat.addInputPath(job2, new Path("temp-mr1"));
		FileOutputFormat.setOutputPath(job2, new Path("temp-mr2"));
		FileInputFormat.addInputPath(job3, new Path("temp-mr2"));
		FileOutputFormat.setOutputPath(job3, new Path("output"));
		
		job1.waitForCompletion(true);
		job2.waitForCompletion(true);
		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Stock_Hadoop-> End**********\n");
	}
}

