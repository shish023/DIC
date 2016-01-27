import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class MapReduce2 {

	public static class Map2 extends Mapper< LongWritable, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


			String line = value.toString(); //receive one line
			String element[] = null;
			element = line.split("\t");
			String stock = element[0].split(",")[0];
			float x = Float.parseFloat(element[1]);

			key1.set(stock);
			value1.set(Float.toString(x));

			context.write(key1, value1);

		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		private Text key2 = new Text();
		private Text value2 = new Text();

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

//			System.out.println("**********"+key.toString());
//			System.out.println("**********"+vol);

			key2 = key;
			value2.set(Double.toString(vol));
			context.write(key2, value2);
		}

	}
}