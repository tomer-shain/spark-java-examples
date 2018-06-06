package spark.java.examples.file;

import org.apache.spark.sql.api.java.UDF1;

public class UpperUDF implements UDF1<String, String> {
 
	private static final long serialVersionUID = -7130446962038498211L;

	@Override
    public String call(String str1) {
      return str1.toUpperCase();
    }
}
