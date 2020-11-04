import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SparkKafkaQ2 {

	public static void main(String[] args) throws InterruptedException {
		//System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop" );
		
		String brokers = args[0];// "52.55.237.11:9092";
		String groupId = args[1];// "asr-pgbde-q2";
		Set<String> topics = Collections.singleton(args[2]); //"stockData"
		String checkpointDir = args[3];

		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.setAppName("SparkKafka")
				.set("spark.streaming.kafka.consumer.cache.enabled", "false");
		// Create streaming context with 1 minute batch interval
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(1));

		jssc.checkpoint(checkpointDir);

		// Set KAFKA parameters for connecting to KAFKA broker
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create an input DStream for Receiving data from KAFKA Topic
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// Set log level to ERROR only.
		Logger.getRootLogger().setLevel(Level.ERROR);

		// Convert JSON message to Java Object of Type CryptoRecord.class(JSON - JAVA Object Mapping)
		JavaDStream<CryptoRecord> cryptoDstream = stream.map(new CryptoRecordMapper());
		
		//cache to workaround KAFKA thread safety errors 
		JavaDStream<CryptoRecord> cryptoDstreamCached = cryptoDstream.cache();
		
		//Create PairDStream (Symbol, PriceDataAvg(OpenPrice, ClosePrice))
		JavaPairDStream<String, PriceDataAvg> pairDstreamPriceData = cryptoDstreamCached
				.mapToPair(d -> new Tuple2<>(d.getSymbol(), new PriceDataAvg(d.getPriceData().getOpen(), d.getPriceData().getClose())));
		JavaPairDStream<String, PriceDataAvg> pairDstreamPriceDataCached = pairDstreamPriceData.cache();
		
		//Do aggregation on DStream to get Avg Opening and Closing Price
		//Here I am using Inverse Function to reduce computation over overlapping window records
		JavaPairDStream<String, PriceDataAvg> resultPairDstream = pairDstreamPriceDataCached
				.reduceByKeyAndWindow(new Function2<PriceDataAvg, PriceDataAvg, PriceDataAvg>() {
					public PriceDataAvg call(PriceDataAvg a, PriceDataAvg b) {
						return PriceDataAvg.sum(a, b);
					}
				}, new Function2<PriceDataAvg, PriceDataAvg, PriceDataAvg>() {
					public PriceDataAvg call(PriceDataAvg a, PriceDataAvg b) {
						return PriceDataAvg.diff(a, b);
					}
				}, Durations.minutes(10), Durations.minutes(5));
		
		JavaPairDStream<String, PriceDataAvg> resultPairDstreamCached = resultPairDstream.cache();
		
		/*-------Logic to get the Currency with maximum profit------------------------*/
		JavaPairDStream<String, Double> joinedDStreamdiff = resultPairDstreamCached
				.mapToPair(d -> new Tuple2<>(d._1, (d._2().getCloseAvg()-d._2().getOpenAvg())));
		//joinedDStreamdiff.print();
		JavaPairDStream<Double, String> swappedPair = joinedDStreamdiff.mapToPair(x -> x.swap());
		JavaPairDStream<Double, String> sortedStream = swappedPair
				.transformToPair(new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> jPairRDD) throws Exception {
						return jPairRDD.sortByKey(false);
					}
				});
		// Answer to Analysis Question 2. Print the Currency Symbol with most profit in last window.
		sortedStream.foreachRDD(rdd -> {
			StringBuilder out = new StringBuilder("\nTop Cryptocurrency to buy:\n");
			for (Tuple2<Double, String> t : rdd.take(1))
				out.append(t._2 +" Profit: "+ t._1().toString()).append("\n");
			System.out.println(out);
		});
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	private static class CryptoRecordMapper implements Function<ConsumerRecord<String, String>, CryptoRecord> {
		private static final ObjectMapper mapper = new ObjectMapper();
		private static final long serialVersionUID = 1L;

		@Override
		public CryptoRecord call(ConsumerRecord<String, String> record) throws Exception {
			return mapper.readValue(record.value(), CryptoRecord.class);
		}
	}

}
