package jsonToAvro;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

public class jsonToAvroMapper extends
		Mapper<NullWritable, Text, LongWritable, Text> {

	String pathToJsonSchema;
	String pathToShortJson;
	String pathToAvro;
	String[] keysForShortJson;
	protected String fileName;

	protected void map(NullWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		setup(context);
		JSONObject jsonObj = stringToJsonObject(value.toString());
		JSONObject shortJsonObject = fillAllFields(jsonObj);
		// super.map(key, value, context);
		context.write(new LongWritable(1),
				new Text(shortJsonObject.toJSONString()));

	}

	private String getCurrentFileName(Context context) {
		FileSplit fileSplit;
		InputSplit is = context.getInputSplit();
		fileSplit = (FileSplit) is;
		fileName = fileSplit.getPath().getName().toString();
		fileName = FilenameUtils.removeExtension(fileName);
		return fileName;
	}

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		pathToJsonSchema = conf.get("pathToJsonSchema");
		pathToShortJson = conf.get("pathToShortJson");
		pathToAvro = conf.get("pathToAvro");
		keysForShortJson = getFieldsFromSchema(pathToJsonSchema);
		fileName = getCurrentFileName(context);

	}

	private String[] getFieldsFromSchema(String pathToSchema) {
		Schema schema = schemaFromString(readFromFile(pathToSchema));
		List<Field> listFields = schema.getFields();
		int listFieldsSize = listFields.size();
		String[] arrayFields = new String[listFieldsSize];
		for (int i = 0; i < listFieldsSize; ++i) {
			arrayFields[i] = listFields.get(i).name();
			// System.out.println(listFields.get(i).name() +
			// " "+listFields.get(i).);
		}
		/*
		 * for (Field oneField : listFields){
		 * System.out.println(oneField.name()+ "!" +
		 * oneField.toString().split(" :")[0]); }
		 */
		return arrayFields;
	}

	public Schema schemaFromString(String schemaStr) {
		Schema.Parser parser = new Schema.Parser();
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

	@SuppressWarnings("unchecked")
	private JSONObject fillAllFields(JSONObject longJsonObj) {

		JSONObject shortJsonObject = new JSONObject();
		for (int i = 0; i < keysForShortJson.length; ++i) {
			String currentKey = keysForShortJson[i].toString();
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

		try {
			FileSystem fs = FileSystem.get(new Configuration());
			Path outputPath = new Path(outputFile + "/uid="
					+ shortJsonObject.get("uid") + ".json");
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					fs.create(outputPath)));
			/*
			 * if (outputFile.startsWith("/")) output = new File(outputFile);
			 * else output = new File(System.getProperty("user.dir") + "/" +
			 * outputFile); if (!output.exists()) output.mkdir();
			 */
			br.write(shortJsonObject.toJSONString());
			br.close();
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

}
