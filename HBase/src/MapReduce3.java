import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapReduce3 {

	public static class Map3 extends TableMapper<DoubleWritable, Text> {
		private DoubleWritable key1 = new DoubleWritable();
		private Text value1 = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

			String stockName = new String(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
			String vol = new String(value.getValue(Bytes.toBytes("vol"), Bytes.toBytes("vol")));

			String element[] = stockName.toString().split(",");
			String stock = element[0];

			key1.set(1.0);     // we can only emit Writables...
			value1.set(stock + "," + vol);

			//System.out.println("**********"+stock);
			//System.out.println("**********"+vol);

			context.write(key1, value1);

		}
	}

	public static class Reduce3 extends Reducer<DoubleWritable, Text, Text, Text> {

		private Text key2 = new Text();
		private Text value2 = new Text();

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String element[] = null;
			String stock;
			Double vol;
			HashMap<String, Double> tmap = new HashMap<String, Double>();

			for (Text value : values) {

				element = value.toString().split(",");
				stock = element[0];
				vol = Double.parseDouble(element[1]);

				if (vol != 0)
					tmap.put(stock, vol);

			}



			ValueComparator bvc =  new ValueComparator(tmap);
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);

			sorted_map.putAll(tmap);


			Set set = sorted_map.entrySet();
			Iterator it = set.iterator();

			String result1 = "\nMost Volatile:\n";
			String result2 = "\nLeast Volatile:\n";

			int n;
			if (sorted_map.size() >= 10)
				n = 10;
			else
				n = sorted_map.size();



			for (int i = 0; i < n; i++) {
				Map.Entry me = (Map.Entry)it.next();
				result1 = result1 + me.getKey() + "\t" + me.getValue().toString() + "\n";
			}


			ArrayList<String> keys = new ArrayList<String>(sorted_map.keySet());
			for (int i = keys.size() - 1; i >= keys.size() - n; i--) {
				String temp = keys.get(i);
				Double v = sorted_map.get(temp);
				String s = keys.get(i);
				result2 = result2 + s + "\t" + Double.toString(v) + "\n";
			}


			key2.set("");
			value2.set(result1 + result2);

			System.out.println("$$$$$$$$" + result1);
			System.out.println("$$$$$$$$" + result2);

			context.write(key2, value2);
		}

	}
}