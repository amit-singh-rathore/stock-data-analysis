import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import org.apache.log4j.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.Optional; 

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;

public class SparkKafkaQ3 {
	public static void main(String[] args) throws InterruptedException {
		//System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop" );
		String brokers = args[0]; //"52.55.237.11:9092";
		String groupId = args[1]; //"asr-pgbde-q3";
		Set<String> topics = Collections.singleton(args[2]); //"stockData"
		String checkpointDir = args[3];

		SparkConf sparkConf = new SparkConf()
				.set("spark.streaming.kafka.consumer.cache.enabled", "false")
				.setAppName("SparkKafka")
				.setMaster("local[*]");
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

		// Convert JSON message to Java Object of Type CryptoRecord.class(JSON - JAVA object Mapping)
		JavaDStream<CryptoRecord> cryptoDstream = stream.map(new CryptoRecordMapper());
		JavaDStream<CryptoRecord> cryptoDstreamCached = cryptoDstream.cache();

		// Create a DSTream with ["BTC", 43968.278], ["BTC", 42016.248]
		JavaPairDStream<String, Double> pairDstreamVolume = cryptoDstreamCached
				.mapToPair(d -> new Tuple2<>(d.getSymbol(), d.getPriceData().getVolume()));

		JavaPairDStream<String, Double> OutVolume = pairDstreamVolume.reduceByKeyAndWindow((a, b) -> a+b,
				Durations.minutes(10), Durations.minutes(10));
		
		/*-------------------------------*/
		// Maintain state as new data comes in and update the state with new data.
        Function2<List<Double>, Optional<Double>, Optional<Double>> updateFunction =
        		(values, state) -> {
        		     double updatedVolume = state.or(0.0);
        		     for (double value : values) {
        		    	 updatedVolume += value;
        		     }
        		     return Optional.of(updatedVolume);
                };

        JavaPairDStream<String, Double> runningVolume = OutVolume.updateStateByKey(updateFunction);

		/*-------------------------------*/

		//Sort the DStream based on Volume.
		JavaPairDStream<Double, String> swappedPair = runningVolume.mapToPair(x -> x.swap());
		JavaPairDStream<Double, String> sortedStream = swappedPair
				.transformToPair(new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> jPairRDD) throws Exception {
						return jPairRDD.sortByKey(false);
					}
				});
		// Print the Cryptocurrency Symbol with most volume traded in last 10 minutes.
		sortedStream.foreachRDD(rdd -> {
			StringBuilder out = new StringBuilder("\nTop Cryptocurrency by volume traded(Symbol, Volume):\n");
			for (Tuple2<Double, String> t : rdd.take(4))
				out.append(t._2()+ " --> "+ t._1().toString()).append("\n");
			System.out.println("-----------------------------");
			System.out.println(out);
			System.out.println("-----------------------------");
		});

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

	// Mapper class for JSON to Java Object conversion
	private static class CryptoRecordMapper implements Function<ConsumerRecord<String, String>, CryptoRecord> {
		private static final ObjectMapper mapper = new ObjectMapper();
		private static final long serialVersionUID = 1L;

		@Override
		public CryptoRecord call(ConsumerRecord<String, String> record) throws Exception {
			return mapper.readValue(record.value(), CryptoRecord.class);
		}
	}
}
