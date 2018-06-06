package spark.java.examples.file;

import java.util.function.Consumer;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleApp {

	protected SparkSession spark;
	protected Logger logger = LoggerFactory.getLogger(ExampleApp.class);

	public static void main(String[] args) {
		(new ExampleApp()).run();
	}
	
	protected void run() {
		System.setProperty("hadoop.home.dir", "path to hadoop-common-2.2.0-bin-master");
		
		spark = SparkSession.builder()
				  .appName("Spark Reader")
				  .master("local")
				  .config("spark.sql.warehouse.dir", "spark-warehouse")
				  .getOrCreate();

		// if you need ot control the file split/partition size (size in bytes)
		// spark.sparkContext().hadoopConfiguration().setInt("mapred.min.split.size", 134217728);
		// spark.sparkContext().hadoopConfiguration().setInt("mapred.max.split.size", 134217728);
		
		Dataset<Row> df = spark.read()
				.schema(Encoders.STRING().schema())
				.format("javatext")
				.load("input path");
		df.printSchema();
		show(df);
		
		// now we can query/parse the data frame 
		
		Dataset<Row> df2 = select(df);
		df2.printSchema();
		show(df2);
	}

	// iterate over a data frame
	protected void show(Dataset<Row> df) {
		df.collectAsList().forEach(new Consumer<Object>() {
			@Override
			public void accept(Object arg0) {
				Row row = (Row) arg0;
				logger.debug(row.toString());
			}
		});
	}
	
	protected Dataset<Row> select(Dataset<Row> df1) {
		Column value = df1.col("value");	// column name is "value"
		Dataset<Row> df2 = df1.select(functions.lower(value).as("lowercaseValue"));
		return df2;
	}
	

	protected Dataset<Row> selectWithUDF(Dataset<Row> df1) {
		spark.udf().register("upperUdf", new UpperUDF(), DataTypes.StringType);
		Column value = df1.col("value");
		Column upper = functions.callUDF("upperUdf", value).as("uppercaseValue");
		Dataset<Row> df2 = df1.select(upper);     // alt: spark.sql("SELECT upperUdf(value)");
		return df2;
	}
}
