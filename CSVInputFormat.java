package org.apache.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import java.io.File;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CSVInputFormat {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Log LOG
        = LogFactory.getLog(CSVInputFormat.class.getName());
		System.out.println("CSVInputFormat is processing");
		LOG.info("CSVInputFormat is processing");
		if (args.length < 1) {
		      System.err.println("Please provide the input file full path as argument");
		      LOG.error("Please provide the input file full path as argument");
		      System.exit(0);
		 }
		
		String csvFile = args[0];
		String DBTable = args[1];
		String sparkMasterUrl = args[2];
		String warehouseLocation = (args[3] != null) ? args[3] : new File("spark-warehouse").getAbsolutePath();
//		LOG.info(warehouseLocation);
		LOG.info("TEsting");
		SparkSession spark = SparkSession
				 .builder().master(sparkMasterUrl)
				  .appName("Java Spark to read embedded new line")
				  .config("spark.sql.warehouse.dir", warehouseLocation)
				  .enableHiveSupport()
				  .getOrCreate();
		
		Dataset<Row> df =  spark.read().format("csv").option("wholeFile", "true").option("sep", ",").option("quote","\"").option("escape","\"").option("multiLine", "true").option("inferSchema", "true").option("header", "true").load(csvFile);
		df.show();
		df.printSchema();
//		df.select("id","name").write().format("orc").save(DBTable);
		
		Dataset<Database> dfDB =  spark.catalog().listDatabases();
		dfDB.show();
//		dfDB.select(col("locationUri")).show();
//		spark.sql("CREATE TABLE "+ DBTable);
//		spark.catalog().listDatabases().show();
		LOG.info(warehouseLocation);
		
	}
	

}
