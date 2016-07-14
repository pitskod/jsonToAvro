package jsonToAvro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class jsonToAvro {

	public static void main(String[] args) throws Exception {

		if (args.length != 5) {
			System.err
					.println("Usage: jsonToAvro <path to input folder with json-files> <path to json schema> "
							+ "<path to folder with temporary short json-files> <path to output folder with avro-files>"
							+ " <path to output folder>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.set("pathToJsonSchema", args[1]);
		conf.set("pathToShortJson", args[2]);
		conf.set("pathToAvro", args[3]);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJobName("Json to Avro");
		job.setJarByClass(jsonToAvro.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setMapperClass(jsonToAvroMapper.class);
		job.setReducerClass(jsonToAvroReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);//avro
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(BytesWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
