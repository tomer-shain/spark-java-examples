


String schemastr = schema.json(); 
StructType schema = (StructType) DataType.fromJson(schemaStr);

StructType schemaString = Encoders.STRING().schema(); 



// driver
ClassTag<String> tagst = scala.reflect.ClassManifestFactory.fromClass(String.class);
Broadcast<String> stringBC = spark.sparkContext().broadcast("hello world", tagst);

// worker
String hello = stringBC.getValue().



// InternalRow from String 
public InternalRow fromString() {
    String line = "hello world";
    List<Object> list = new LinkedList<Object>();
    list.add(UTF8String.fromString(line));
    InternalRow row = InternalRow.fromSeq(JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq());
    return row;
}



// Scala Map to Java Map
scala.collections.immutable.Map<String, String> scmap = new scala.collection.immutable.HashMap<>(); 
java.util.Map<String, String> jmap =JavaConverters.mapAsJavaMapConverter(scmap).asJava(); 

// Seq to Java iterator
Seq<InternalRow> seqrows = ...
Iterator<InternalRow> javarows = JavaConverters.asJavaCollectionConverter(seqrows).asJavaCollection().iterator();
