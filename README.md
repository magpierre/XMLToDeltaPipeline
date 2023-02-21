# XMLToDeltaPipeline
An example of how you can parse XML systematically and populate Delta Lake tables without spark.

cat xmldoc | java -jar XMLPreprocessor/target/uber-XMLPreprocessor-1.0-SNAPSHOT.har | java -jar StdInToDeltaWriter/target/standalone-StdInToDeltaWriter-1.0-SNAPSHOT.jar s3a://<path to target delta table> <path to core-site.xml>
