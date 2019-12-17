package app;

import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import app.cassandra.CassandraMain;
import app.entities.Article;
import app.entities.Words;
import app.helpers.CassandraProperties;
import app.helpers.KafkaProperties;
import app.helpers.NewsClientParams;
import app.kafka.Kafka;
import app.rest.Client;
import app.spark.kafkaSparkConnector;

public class Application {

	private static Collection<String> topics;
	private static String topicName = "TopicA";
	
	public static void main(String[]args) {
		
		CassandraMain cass = new CassandraMain(CassandraProperties.hostName, CassandraProperties.clusterName, 0);
		cass.createKeySpace(CassandraProperties.keySpaceName);
		cass.session.execute("CREATE TABLE IF NOT EXISTS news_Words (word text, occurences int, PRIMARY KEY(word));");		
		
		//Mapper doesn't work with a Spark context. Thus using a Spark-Cassandra connector
		//MappingManager mp = cass.createMappingManager();		
		//Mapper<Words> mapper = mp.mapper(Words.class);
		
		NewsClientParams newsParams = new NewsClientParams();
		Client newsClients = new Client(newsParams.baseUri, newsParams.newsParams, newsParams.topHeadlines);
		ArrayList<String> listName = newsClients.response.getBody().jsonPath().getJsonObject("'articles'.'source'.'name'");
		ArrayList<String> listAuthor = newsClients.response.getBody().jsonPath().getJsonObject("'articles'.'author'");
		ArrayList<String> listTitle = newsClients.response.getBody().jsonPath().getJsonObject("'articles'.'title'");

		topics = Arrays.asList(topicName);
		
		KafkaProperties props = new KafkaProperties();		
		Kafka kafka = new Kafka(topicName);
		KafkaProducer<String, String> producer = kafka.createProducer(props.KafkaProducerProperties());

		kafkaSparkConnector spark = new kafkaSparkConnector();

		JavaPairDStream<String, String> kafkaStream = spark.createKafkaDirectStream(topics);		

		spark.processKafkaMessage(kafkaStream)
			.foreachRDD(rdd->{
								System.out.println("--- New RDD with " + rdd.partitions().size()
										+ " partitions and " + rdd.count() + " records" + " topic=" + rdd.name());
								
								Map<String, Integer> wordCountMap = rdd.collectAsMap();
								
								for(String key : wordCountMap.keySet()) {
									List<Words> wordList = Arrays.asList(new Words(key.replaceAll("[^a-zA-Z]+",""), wordCountMap.get(key)));
									JavaRDD<Words> rddWord = spark.sparkContext.streamingContext.sparkContext().parallelize(wordList);
							        CassandraJavaUtil.javaFunctions(rddWord).writerBuilder(CassandraProperties.keySpaceName,
							        		"news_Words", CassandraJavaUtil.mapToRow(Words.class)).saveToCassandra();
							        //mapper.save(rddW));
							    }
								
								rdd.foreach(record ->System.out.println(record));
							});

		spark.sparkContext.streamingContext.start();
		
		for(int i = 0; i < listName.size(); i++) {
			Article article = new Article(listName.get(i) != null ? listName.get(i): "noCompany",
											listAuthor.get(i) != null ? listAuthor.get(i) : "noAuthor", listTitle.get(i));
			try {
				kafka.runKafkaProducer(article, producer);
				Thread.sleep(550);
			} catch (NotSerializableException e) {
				System.out.println("NotSerializableException:");
				e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println("InterruptedException:");
				e.printStackTrace();
			}
		}
		
		try {
			spark.sparkContext.streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("InterruptedException");
			e.printStackTrace();
		}
		
		spark.sparkContext.streamingContext.stop();
		producer.close();
		//String selectQuery = "SELECT * FROM " + CassandraProperties.keySpaceName + "." + "news_Words"; 
		//System.out.println("news_Words CONTAINS:\n" + cass.session.execute(selectQuery));
		cass.closeConnection();
	}
}