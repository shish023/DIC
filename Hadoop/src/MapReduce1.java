
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapReduce1 {
    public static class Map1
        extends Mapper<LongWritable, Text, Text, Text> {
        private Text key1 = new Text();
        private Text value1 = new Text();

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
            if (key.get() >= 1L) {
                String line = value.toString();
                String[] element = null;
                element = line.split(",");
                String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
                String year_month = element[0].substring(0, 7);
                String day = element[0].substring(8, 10);
                int dotIndex = fileName.lastIndexOf('.');
                String stock = fileName.substring(0, dotIndex);

                this.key1.set(stock + "," + year_month);
                this.value1.set(day + "," + element[6]);

                context.write(this.key1, this.value1);
            }
        }
    }

    public static class Reduce1
        extends Reducer<Text, Text, Text, Text> {
        private Text key2 = new Text();
        private Text value2 = new Text();

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
            String[] element = null;
            int date = 0;
            int startDate = 32;
            int endDate = 0;
            float close = 0.0F;
            float endClose = 0.0F;
            float startClose = 0.0F;
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
            float x;
            float x;
            if (startClose == 0.0F) {
                x = 0.0F;
            } else {
                x = (endClose - startClose) / startClose;
            }
            this.key2 = key;
            this.value2.set(Float.toString(x));
            context.write(this.key2, this.value2);
        }
    }
}