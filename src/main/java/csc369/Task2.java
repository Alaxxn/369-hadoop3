package csc369;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;





public class Task2 {

    // Final output: <(Country, URL), visit_count>
    public static final Class OUTPUT_KEY_CLASS = Text.class; 
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Map<String, String> ipToCountry = new HashMap<>();
        private final static IntWritable one = new IntWritable(1);
        private Text countryUrlKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException { // Loads hostname_country Data
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    java.io.File cachePath = new java.io.File(cacheFile.getPath());

                    if (cachePath.isDirectory()) {
                        for (java.io.File file : cachePath.listFiles()) {
                            if (file.getName().endsWith(".csv")) {
                                BufferedReader reader = new BufferedReader(new FileReader(file));
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    String[] parts = line.split(",");
                                    if (parts.length == 2) {
                                        ipToCountry.put(parts[0].trim(), parts[1].trim());
                                    }
                                }
                                reader.close();
                            }
                        }
                    } else {
                        BufferedReader reader = new BufferedReader(new FileReader(cachePath));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",");
                            if (parts.length == 2) {
                                ipToCountry.put(parts[0].trim(), parts[1].trim());
                            }
                        }
                        reader.close();
                    }
                }
            }
        }

        
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] data = line.split(" ");

            if (data.length > 1) { 
                String host = data[0];
                String request = line.split("\"")[1]; // extract what's inside quotes
                String url = request.split(" ")[1];   // URL
                String country = ipToCountry.getOrDefault(host, "Unknown Location");
                countryUrlKey.set(country + "\t" + url);
                context.write(countryUrlKey, one);
            }
            }


    } // End of Mapper

    // Reducer: sum all the counts per country
    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable output_sum = new IntWritable();
        
        @Override
        public void reduce(Text CountryAndUrl, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            output_sum.set(sum);
            context.write(CountryAndUrl, output_sum);
        }

    } // End of Reducer
    
}
