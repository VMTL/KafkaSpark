package app.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingContext {
	
	public JavaStreamingContext streamingContext;
    
    public StreamingContext() {
    	SparkConf sparkConf = new SparkConf()
    		.setAppName("NewsParsing")
    		.setMaster("local[*]")
    		.set("spark.streaming.stopGracefullyOnShutdown", "true")
    		.set("spark.cassandra.connection.host", "127.0.0.1")

    		//This will create 3 partitions with 2GB of RAM per each
    		.set("spark.cores.max", "6")
    		.set("spark.executor.cores", "2")
    		.set("spark.executor.memory", "2GB")
    	
    		//.set("spark.locality.wait", "100")
    		.set("spark.streaming.backpressure.enabled", "true")
    		.set("spark.streaming.kafka.consumer.poll.ms", "512");
    	 
    	this.streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
    	//streamingContext.checkpoint("./.checkpoint");
    }
}