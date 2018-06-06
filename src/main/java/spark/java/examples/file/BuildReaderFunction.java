package spark.java.examples.file;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

public class BuildReaderFunction extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

	private static final long serialVersionUID = -7858535696852020148L;
	Broadcast<SerializableConfiguration> broadcastedHadoopConf;
	
	public BuildReaderFunction(SparkSession sparkSession, StructType dataSchema, StructType partitionSchema,
			StructType requiredSchema, Seq<Filter> filters, Map<String, String> options, Configuration hadoopConf) {
		super();
		ClassTag<SerializableConfiguration> tagconf = scala.reflect.ClassManifestFactory.fromClass(SerializableConfiguration.class);
		broadcastedHadoopConf = sparkSession.sparkContext().broadcast(new SerializableConfiguration(hadoopConf), tagconf);
	}


	@Override
	public Iterator<InternalRow> apply(PartitionedFile file) {
		Configuration hadoopConf = broadcastedHadoopConf.getValue().value();
		return new FileReader(file, hadoopConf);
	}

}
