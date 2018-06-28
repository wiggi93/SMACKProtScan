package fastaParser;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.impl.Log4jLoggerFactory;

import com.datastax.spark.connector.cql.CassandraConnector;

import cassandra.CassandraConnection;
import scala.Tuple2;

public class SparkJob {

	public static boolean first = true;
	public static long timeStart, timeEnd;
	
	public final static String KAFKA_URL = "192.168.0.21:9092,192.168.0.25:9092";//64
	public final static String CASSANDRA_URL = "192.168.0.21";//64
	public final static String KAFKA_TOPIC = "fasta";
	
	public static JavaSparkContext javaSparkContext;

	
	public static void main(String[] args) throws Exception{
		
		Logger log = new Log4jLoggerFactory().getLogger("");
		
		SparkConf sparkConf = new SparkConf().setAppName("SMACKProtScan");	
		
		SparkSession session = SparkSession.builder()
		.appName("SMACKProtScan")
		.config(sparkConf)
		.getOrCreate();

		javaSparkContext = JavaSparkContext.fromSparkContext(session.sparkContext());
		JavaStreamingContext context = new JavaStreamingContext(javaSparkContext, new Duration(2000));
		
		javaSparkContext.setLogLevel("WARN");
		
        CassandraConnector connector = CassandraConnector.apply(sparkConf);
        CassandraConnection cassandraConnection = new CassandraConnection(javaSparkContext, connector);
        cassandraConnection.createDB();     
        
        log.warn("huuuuurensohn");
        
        Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_URL);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "fasta");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList(KAFKA_TOPIC);
 
		final JavaInputDStream<ConsumerRecord<String, String>> dstream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
				
		dstream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
			private static final long serialVersionUID = 601114341833848854L;
			@Override
		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				log.warn(record.value());
				return new Tuple2<>(record.key(), record.value());
		    }
		});	
		dstream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>(){
			private static final long serialVersionUID = 1039948106556530621L;
			@Override
			  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
				  final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				  rdd.foreach(new VoidFunction<ConsumerRecord<String, String>>() {

					private static final long serialVersionUID = 1645460827458322875L;
					@Override
					  public void call(ConsumerRecord<String, String> consumerRecord) throws Exception {
						  log.warn("consumerreccord");
						  log.warn(consumerRecord.value());
						  log.warn("bitte was");
						  OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
						  cassandraConnection.computeCassandraOperations(consumerRecord);
					  }
				  });
			  }
		});
		
		context.start();
		
		try {
			context.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 

		cassandraConnection.closeCluster();
	}
}
	
	
