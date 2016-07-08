package jsonToAvro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class jsonToAvro {
	

	public static void main(String[] args) throws Exception {
		/*String[] args1 = {"/home/cloudera/Json/shortJsonToAvro/input1","/home/cloudera/Json/shortJsonToAvro/shortSchema.avsc",
		//		"/home/cloudera/Json/shortJsonToAvro/shortJson","/home/cloudera/Json/shortJsonToAvro/avro1","/home/cloudera/Json/shortJsonToAvro/output1"};
		args= args1;*/
		
		
		
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
		conf.set("keysForShortJson", "uid,first_name,last_name,sex,bdate");
		
		Job job = new Job(conf);
		job.setJobName("Json to Avro");
		job.setJarByClass(jsonToAvro.class);

		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);//avro
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(jsonToAvroMapper.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
// ^^ MaxTemperature
