package jsonToAvro;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class jsonToAvroReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	String pathToJsonSchema;
	String pathToAvro;

	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		jsonToAvro(pathToJsonSchema, values);

	}

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		pathToJsonSchema = conf.get("pathToJsonSchema");
		pathToAvro = conf.get("pathToAvro");

	}

	private void jsonToAvro(String pathToSchema, Iterable<Text> values) {

		Schema schema = schemaFromString(readFromFile(pathToSchema));
		// System.out.println(schema);
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			fs.mkdirs(new Path(pathToAvro));
			Path pathToFile = new Path(pathToAvro + "/" + "all" + ".avro");
			OutputStream out;
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
					schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
					writer);
			out = fs.create(pathToFile);
			dataFileWriter.create(schema, out);
			GenericRecord datum = null;
			for (Text json : values) {
				datum = new GenericData.Record(schema);
				JSONObject jsonObj = stringToJsonObject(json.toString());
				for (Object key : jsonObj.keySet()) {
					String keyStr = key.toString();
					// System.out.println(keyStr + " "+ jsonObj.get(keyStr));
					datum.put(keyStr, jsonObj.get(keyStr));
				}
				dataFileWriter.append(datum);
			}
			dataFileWriter.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	public JSONObject stringToJsonObject(String jsonString) {
		JSONParser parser = new JSONParser();
		Object obj = null;
		try {
			obj = parser.parse(jsonString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (JSONObject) obj;
	}

	public Schema schemaFromString(String schemaStr) {
		Schema.Parser parser = new Schema.Parser();
		// System.out.println(getClass().getResourceAsStream("shortSchema.avsc").);
		Schema schema = parser.parse(schemaStr);
		return schema;
	}

	public String readFromFile(String pathToSchema) {

		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(new Path(pathToSchema))));
			String schemaStr = "";
			String line;
			while ((line = br.readLine()) != null)
				schemaStr += line;
			br.close();
			return schemaStr;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

}
