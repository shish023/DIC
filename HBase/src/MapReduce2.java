import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MapReduce2 {

	public static class Map2 extends TableMapper<Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {


			String stockName = new String(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
			String xi = new String(value.getValue(Bytes.toBytes("xi"), Bytes.toBytes("xi")));

			String element[] = stockName.toString().split(",");
			String stock = element[0];

			key1.set(stock);     // we can only emit Writables...
			value1.set(xi);

			//System.out.println("**********"+stock);
			//System.out.println("**********"+xi);

			context.write(key1, value1);

		}
	}

	public static class Reduce2 extends TableReducer<Text, Text, ImmutableBytesWritable>  {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			float x;
			double sum = 0;
			int count = 0;
			String element = null;
			List<Float> list = new ArrayList<Float>();


			for (Text value : values) {
				element = value.toString();
				x = Float.parseFloat(element);
				sum = sum + x;
				count++;

				list.add(x);

			}

			double avg = sum / count;

			double sub = 0;
			double sq = 0;
			double sum2 = 0;

			for (Float entry : list) {
				sub = entry.floatValue() - avg;
				sq = sub * sub;
				sum2 = sum2 + sq;

			}

			double vol;

			if (count == 1)
				vol = 0;
			else
				vol = Math.sqrt(sum2 / (count - 1));

			String v = Double.toString(vol);
			byte[] xiByte = Bytes.toBytes(v);
			byte[] stockByte = Bytes.toBytes(key.toString());
			byte[] rowid = Bytes.toBytes(key.toString());


			Put p = new Put(rowid);
			p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), stockByte);
			p.add(Bytes.toBytes("vol"), Bytes.toBytes("vol"), xiByte);


			context.write(null, p);
		}

	}
}