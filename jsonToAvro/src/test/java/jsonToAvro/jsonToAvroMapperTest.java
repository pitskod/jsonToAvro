package jsonToAvro;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.junit.*;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class jsonToAvroMapperTest {

	jsonToAvroMapper jstoav = new jsonToAvroMapper();

	String pathToJsonSchema = "src/test/resources/input/shortSchema.avsc";
	String pathToShortJson = "src/test/resources/output/shortJsonFiles/uid=1.json";
	String pathToAvro = "src/test/resources/output/avroFiles/all.avro";

	@SuppressWarnings("resource")
	@Test
	public void checkSchema() {
		File avroFile = new File(pathToAvro);
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		try {
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
					avroFile, reader);
			assertEquals((Object) jstoav.schemaFromString(jstoav
					.readFromFile(pathToJsonSchema)),
					(Object) dataFileReader.getSchema());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void checkJsonObjects() {
		File avroFile = new File(pathToAvro);
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader;
		JSONObject rightJsonObject = jstoav.stringToJsonObject(jstoav
				.readFromFile(pathToShortJson));

		try {
			dataFileReader = new DataFileReader<GenericRecord>(avroFile, reader);
			assertTrue(dataFileReader.hasNext());
			Object resultJson = dataFileReader.next();
			JSONAssert.assertEquals(rightJsonObject.toJSONString().toString(),
					resultJson.toString(), JSONCompareMode.NON_EXTENSIBLE);
			assertFalse(dataFileReader.hasNext());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
