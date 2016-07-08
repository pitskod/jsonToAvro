package jsonToAvro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.hadoop.mapreduce.InputSplit;

public class jsonToAvroMapper extends
		Mapper<NullWritable, Text, Text, IntWritable> {
	String pathToJsonSchema;
	String pathToShortJson;
	String pathToAvro;
	String[] keysForShortJson;
	protected String fileName;

	@Override
	protected void map(NullWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		configure(context);
		JSONObject jsonObj = stringToJsonObject(value.toString());
		JSONObject shortJsonObject = fillAllFields(jsonObj);
		File avroFolder = new File(pathToAvro);
		avroFolder.mkdir();
		jsonToAvro(pathToJsonSchema, shortJsonObject.toString(),
				getCurrentFileName(context));
		super.map(key, value, context);

	}

	private String getCurrentFileName(Context context) {
		FileSplit fileSplit;
		InputSplit is = context.getInputSplit();

		fileSplit = (FileSplit) is;
		String fileName = fileSplit.getPath().getName().toString();

		return FilenameUtils.removeExtension(fileName);
	}

	public void configure(Context context) {
		Configuration conf = context.getConfiguration();
		pathToJsonSchema = conf.get("pathToJsonSchema");
		pathToShortJson = conf.get("pathToShortJson");
		pathToAvro = conf.get("pathToAvro");
		keysForShortJson = stringToArray(conf.get("keysForShortJson"));
		// fileName = context.getConfiguration().get("map.input.file");
		// System.out.println(fileName);

	}

	private String[] stringToArray(String string) {
		String delims = ",";
		String[] element = string.split(delims);
		return element;
	}

	private void jsonToAvro(String pathToSchema, String json, String avro) {

		Schema schema = schemaFromString(readFromFile(pathToSchema));

		GenericRecord datum = new GenericData.Record(schema);
		JSONObject jsonObj = stringToJsonObject(json);
		for (int i = 0; i < keysForShortJson.length; ++i) {
			datum.put(i, jsonObj.get(keysForShortJson[i]));
		}
		File file = new File(pathToAvro + "/" + avro + ".avro");
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
				schema);
		// Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				writer);
		try {
			dataFileWriter.create(schema, file);
			dataFileWriter.append(datum);
			dataFileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Schema schemaFromString(String schemaStr) {
		Schema.Parser parser = new Schema.Parser();
		// System.out.println(getClass().getResourceAsStream("shortSchema.avsc").);
		Schema schema = parser.parse(schemaStr);
		return schema;
	}

	public String readFromFile(String pathToSchema) {
		File f = new File(pathToSchema);
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(f));
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

	@SuppressWarnings("unchecked")
	private JSONObject fillAllFields(JSONObject longJsonObj) {

		JSONObject shortJsonObject = new JSONObject();
		for (int i = 0; i < keysForShortJson.length; ++i) {
			String currentKey = keysForShortJson[i];
			if (longJsonObj.containsKey(currentKey)) {
				shortJsonObject.put(currentKey, longJsonObj.get(currentKey));
			} else {
				shortJsonObject.put(currentKey, "NULL");
			}
		}
		writeToFolder(shortJsonObject, pathToShortJson);
		return shortJsonObject;

	}

	private void writeToFolder(JSONObject shortJsonObject, String outputFile) {
		File output;
		if (outputFile.startsWith("/"))
			output = new File(outputFile);
		else
			output = new File(System.getProperty("user.dir") + "/" + outputFile);
		if (!output.exists())
			output.mkdir();
		FileWriter file;
		try {
			file = new FileWriter(outputFile + "/uid="
					+ shortJsonObject.get("uid") + ".json");
			file.write(shortJsonObject.toJSONString());
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

}
