package csc369;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*
Flips <country, sum> to <sum, key>
*/

public class Task1Sort {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable output_sum = new IntWritable();
        private Text output_country = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split("\\t", 2);
            if (data.length == 2) {
                output_sum.set(Integer.parseInt(data[1].trim()));
                output_country.set(data[0].trim());
                context.write(output_sum, output_country);
            }
        }
    }

    // Reducer: sum all the counts per country
    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {
        
        @Override
        public void reduce(IntWritable sum, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Text country = values.iterator().next();
            context.write(sum, country);
        }
    }

    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable k1 = (IntWritable) w1;
            IntWritable k2 = (IntWritable) w2;
            return -1 * k1.compareTo(k2); // reverse order
        }
    }

}
