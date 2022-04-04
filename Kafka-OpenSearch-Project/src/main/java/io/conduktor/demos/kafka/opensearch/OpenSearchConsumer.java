package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "https://573at2t7s6:cgbpfljn7p@kafka-wikimedia-3902144086.us-east-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        String bootstrapServer = "localhost:9094";
        String groupId = "consumer-opensearch-demo";

        //Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Manual Offset
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //Create Consumer
         return new KafkaConsumer<String, String>(properties);
    }

    private static String extractId(String json) {
        //gson library
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        //first create an OpenSearch Client
        RestHighLevelClient openSearchClient =  createOpenSearchClient();

        //Create Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        //We need to create index on opensearch if it doesn't exist already
       try(openSearchClient; consumer) {
           boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

           if (!indexExist) {
               CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
               openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
               log.info("Wikimedia Index has been created!");
           } else {
               log.info("Wikimedia Index already exist!");
           }

           consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

           while (true) {
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

               int recordCount = records.count();
               log.info("Recived " + recordCount + " records");

               BulkRequest bulkRequest = new BulkRequest();

               for (ConsumerRecord<String, String> record : records) {


                   //Try Catch used to get rid off --> type=mapper_parsing_exception, reason=object mapping for [log_params] tried to parse field [null] as object, but found a concrete value
                   try{

                       //We extract the ID from the JSON value(Idemptotant)
                       String id = extractId(record.value());

                       IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

                       //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                       bulkRequest.add(indexRequest);

                       //log.info(response.getId());
                   }catch (Exception e){

                   }
               }

               if (bulkRequest.numberOfActions() > 0) {
                   BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                   log.info("Inserted "+bulkResponse.getItems().length+" records.");

                   try{
                       //if we remove the sleep it will work much faster
                       Thread.sleep(1000);
                   }catch (Exception e){
                       e.printStackTrace();
                   }
                   //commit offset
                   consumer.commitSync();
                   log.info("Offset has been commited!");
               }
           }

       }
    }
}
