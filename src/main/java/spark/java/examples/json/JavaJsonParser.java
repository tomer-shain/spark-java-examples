package spark.java.examples.json;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonParser;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import scala.collection.Seq;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

public class JavaJsonParser {

    //
	// JSON parser
    //
    
    protected Seq<InternalRow> parse(JacksonParser parser, String line) {
		String record = line;
		return parser.parse(record, new JsonParserFactory(), new RecordLiteral());
	}
	
	private class JsonParserFactory extends AbstractFunction2<JsonFactory, String, JsonParser> {
		@Override
		public JsonParser apply(JsonFactory factory, String data) {
			try {
				return factory.createParser(data);
			} catch (IOException e) {
				// log parsing warning and continue to next record;
				// log.error("JSON parsing exception: " + data);
				try {
					return factory.createParser("{parsing exception}");
				} catch (Exception e1) {
					throw new RuntimeException("JSON parsing exception", e);
				}
			}
		}
	}

	private class RecordLiteral extends AbstractFunction1<String, UTF8String> {
		@Override
		public UTF8String apply(String javastr) {
			return UTF8String.fromString(javastr);
		}
	}
	
	/**
	 * @param schema	performance: pass the "required schema" to (vs dataSchema)
	 * @return
	 */
	protected JacksonParser getJacksonParser(StructType schema) {
		return new JacksonParser(schema, null);
	}

	

}
