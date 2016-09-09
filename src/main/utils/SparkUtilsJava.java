package main.utils;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by chandan on 09/09/16.
 */
public class SparkUtilsJava {
    static Logger logger = Logger.getLogger(SparkUtilsJava.class.getName());

    /*
         returns an Injection object which makes conversion from/to binary easy.
         For more details, refer twitter bijection.
         uses the kafka topic name to get the avro schema and then create the injection object.
     */
    public static Injection<GenericRecord, byte[]> getRecordInjection(String topic) throws Exception {
        Schema avroSchema = getLatestSchemaByTopic(topic);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(avroSchema);
        return recordInjection;
    }

    /*
        get avro schema for the kafka topic
     */
    public static Schema getLatestSchemaByTopic(String topicName) throws Exception {
        String url = Constants.KAFKA_SCHEMA_REGISTRY_URL;//TODO move to config file
        HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
        String schemaUrl = url + "/" + topicName + "/latest";
        logger.info("getLatestSchemaByTopic : Fetching from URL : " + schemaUrl);
        GetMethod get = new GetMethod(schemaUrl);

        int statusCode;
        String responseBody;
        try {
            statusCode = httpClient.executeMethod(get);
            responseBody = get.getResponseBodyAsString();
        } catch (HttpException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            get.releaseConnection();
        }

        if (statusCode != HttpStatus.SC_OK) {
            throw new Exception(
                    String.format("Latest schema for topic %s cannot be retrieved. Status code = %d", topicName, statusCode));
        }

        Schema schema;
        try {
            String schemaString = responseBody.substring(responseBody.indexOf('{'));
            schema = new Schema.Parser().parse(schemaString);
            logger.info("getLatestSchemaByTopic : returning schema= " + schema);
        } catch (Throwable t) {
            throw new Exception(String.format("Latest schema for topic %s cannot be retrieved", topicName), t);
        }
        return schema;
    }



}
