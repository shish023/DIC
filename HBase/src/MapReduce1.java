import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapReduce1 {
	/**
	 * @author ruhansa
	 * read files line by line, put the data into hbase style table
	 * input: <key, value>, key: line number, value: line
	 * output: <key, value>, key: rowid, value: hbase row content
	 */
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			String line = value.toString(); //receive one line
			String element[] = null;
			element = line.split(",");
			if (element[0].trim().compareTo("Date") != 0) {
				String dates[] = element[0].split("-");
				String yr = dates[0];
				String mm = dates[1];
				String dd = dates[2];
				String price = element[6];
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				String stockName = fileName.substring(0, fileName.length() - 4);
				String file = stockName;

				key1.set(file + "," + yr + "," + mm);
				value1.set(dd + "," + price);

				try {
					context.write(key1, value1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}


	public static class Reduce1 extends TableReducer<Text, Text, ImmutableBytesWritable> {

		private Text key2 = new Text();
		private Text value2 = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String element[] = null;
			int date = 0;
			int startDate = 32;
			int endDate = 0;
			float close = 0;
			float endClose = 0;
			float startClose = 0;

			for (Text value : values) {
				element = value.toString().split(",");
				date = Integer.parseInt(element[0]);
				close = Float.parseFloat(element[1]);

				if (date > endDate) {
					endDate = date;
					endClose = close;
				}

				if (date < startDate) {
					startDate = date;
					startClose = close;
				}

			}

			//System.out.println("**********"+key.toString());
			//System.out.println("**********"+startClose);
			//System.out.println("**********"+endClose);

			float x;

			if (startClose == 0)
				x = 0;
			else
				x = (endClose - startClose) / startClose;
			//System.out.println("**********"+x);

			String stockName = key.toString();
			byte[] stockByte = Bytes.toBytes(stockName);

			String xi = Float.toString(x);
			byte[] xiByte = Bytes.toBytes(xi);

			byte[] rowid = Bytes.toBytes(key.toString());

			Put p = new Put(rowid);
			p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), stockByte);
			p.add(Bytes.toBytes("xi"), Bytes.toBytes("xi"), xiByte);

			try {
				context.write(new ImmutableBytesWritable(rowid), p);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}