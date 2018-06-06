package spark.java.examples.file;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.HadoopFileLinesReader;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.unsafe.types.UTF8String;

import scala.collection.AbstractIterator;
import scala.collection.JavaConverters;

public class FileReader extends AbstractIterator<InternalRow> {

	HadoopFileLinesReader lineReader;
	
	public FileReader(PartitionedFile file, Configuration hadoopConf) {
        lineReader = new HadoopFileLinesReader(file, hadoopConf);

        // if you prefer an input stream:
		// Path path = new Path(file.filePath());
    	// InputStream inputStream = path.getFileSystem(hadoopConf).open(path);
    	// 
		// do not forget the file partition limits: 
    	// seek to file.strart() and
    	// stop after reaching (file.start() + file.length())
	}

	@Override
	public boolean hasNext() {
		return lineReader.hasNext();
	}
	
	@Override
	public InternalRow next() {
		List<Object> list = new LinkedList<Object>();
		list.add(UTF8String.fromString(lineReader.next().toString()));
		InternalRow row = InternalRow.fromSeq(JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq());
		return row;
	}
	
}
