# jsonToAvro
Для запуска в eclipse: 
run as/run configurations/arguments/progran arguments:
("Usage: jsonToAvro 	<path to input folder with json-files> 
			<path to json schema>
			<path to folder with temporary short json-files> 
			<path to output folder with avro-files>"
			<path to output folder>")

src/main/resources/input/jsonFiles src/main/resources/input/vkuser.avsc  src/main/resources/output/shortJsonFiles src/main/resources/output/avroFiles src/main/resources/output/jobOutput

Для сборки jar:
Maven build .. 
Goals : package -DskipTests

Для запуска jar:
hadoop jar  /home/cloudera/workspace/jsonToAvro/target/jsonToAvro-0.0.1-SNAPSHOT-jar-with-dependencies.jar jsonToAvro/input/jsonFiles jsonToAvro/input/vkuser.avsc  jsonToAvro/output/shortJsonFiles jsonToAvro/output/avroFiles jsonToAvro/output/jobOutput


До запуска jar :
(закинуть папку input в hdfs)
hadoop fs -mkdir /user/cloudera/jsonToAvro
hadoop fs -put /home/cloudera/workspace/jsonToAvro/src/main/resources/input/ /user/cloudera/jsonToAvro/input






