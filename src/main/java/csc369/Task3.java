package csc369;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;




public class Task3 {

    // Final output: <(URL, Country)>
    public static final Class OUTPUT_KEY_CLASS = Text.class; 
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

        private Map<String, String> ipToCountry = new HashMap<>();
        private Text output_key = new Text();
        private Text output_value = new Text();

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
                output_key.set(url);
                output_value.set(country);
                context.write(output_key, output_value);
            }
            }


    } // End of Mapper

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {

        private Text outputValue = new Text();

        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> uniqueCountries = new HashSet<>();

            for (Text val : values) {
                uniqueCountries.add(val.toString());
            }

            StringJoiner joiner = new StringJoiner(", ");
            for (String country : uniqueCountries) {
                joiner.add(country);
            }
            outputValue.set(joiner.toString());
            context.write(key, outputValue);
        }

    } // End of Reducer
    
}
