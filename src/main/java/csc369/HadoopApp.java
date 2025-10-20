package csc369;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	}  else if ("Test".equalsIgnoreCase(otherArgs[0])) { // TEST JOB
	    job.setReducerClass(Test.ReducerImpl.class);
	    job.setMapperClass(Test.MapperImpl.class);
	    job.setOutputKeyClass(Test.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Test.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Task1".equalsIgnoreCase(otherArgs[0])) { // Task1
		job.setReducerClass(Task1.ReducerImpl.class);
		job.setMapperClass(Task1.MapperImpl.class);
		job.setOutputKeyClass(Task1.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task1.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1])); // access.log
		job.addCacheFile(new Path(otherArgs[2]).toUri()); // hostname_country.csv
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	}else if ("Task1Sort".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Task1Sort.ReducerImpl.class);
		job.setMapperClass(Task1Sort.MapperImpl.class);
		job.setOutputKeyClass(Task1Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task1Sort.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.setSortComparatorClass(Task1Sort.DescendingIntComparator.class); //Descending order
	}
	else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
