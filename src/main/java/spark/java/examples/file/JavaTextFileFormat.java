package spark.java.examples.file;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.TextBasedFileFormat;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function1;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.Map;


public class JavaTextFileFormat extends TextBasedFileFormat implements DataSourceRegister, Serializable {

	Logger logger = LoggerFactory.getLogger(JavaTextFileFormat.class);
	
	@Override
	public String shortName() {
		return "javatext";
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.sql.execution.datasources.FileFormat#inferSchema(org.apache.spark.sql.SparkSession, scala.collection.immutable.Map, scala.collection.Seq)
	 * This example creates a simple data-frame with a single string value in each row. So no schema
	 * inference is not really required. Replace with your library schema inference code if needed.
	 */
	@Override
	public Option<StructType> inferSchema(SparkSession arg0, Map<String, String> arg1, Seq<FileStatus> arg2) {
		StructType schema = Encoders.STRING().schema();
		return new scala.Some<StructType>(schema);
	}

	@Override
	public OutputWriterFactory prepareWrite(SparkSession arg0, Job arg1, Map<String, String> arg2, StructType arg3) {
		// TODO not implemented
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.sql.execution.datasources.TextBasedFileFormat#buildReader(org.apache.spark.sql.SparkSession, org.apache.spark.sql.types.StructType, org.apache.spark.sql.types.StructType, org.apache.spark.sql.types.StructType, scala.collection.Seq, scala.collection.immutable.Map, org.apache.hadoop.conf.Configuration)
	 * buildReader is factory of factory method. 
	 * Called by the Spark driver, returns a serializeable factory function.
	 * The factory function is instantiated by the driver, serialized and distributed to the executors. 
	 * When an executor need an instance of the file reader, it uses the factory function.   
	 */
	@Override
	public Function1<PartitionedFile, Iterator<InternalRow>> buildReader(SparkSession sparkSession,
			StructType dataSchema, StructType partitionSchema, StructType requiredSchema, Seq<Filter> filters,
			Map<String, String> options, Configuration hadoopConf) {
		return new BuildReaderFunction(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf);
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.sql.execution.datasources.TextBasedFileFormat#isSplitable(org.apache.spark.sql.SparkSession, scala.collection.immutable.Map, org.apache.hadoop.fs.Path)
	 */
	@Override
	public boolean isSplitable(SparkSession sparkSession, Map<String, String> options, Path path) {
		return true;
	}

}
