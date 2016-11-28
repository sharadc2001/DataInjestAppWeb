/**
 * Copyright 2015 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/**
 * Licensed Materials - Property of IBM
 * (c) Copyright IBM Corp. 2015
 */
package com.messagehub.samples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messagehub.samples.env.CreateTopicConfig;
import com.messagehub.samples.env.CreateTopicParameters;
import com.messagehub.samples.env.MessageHubCredentials;
import com.messagehub.samples.env.MessageHubEnvironment;
import com.messagehub.samples.env.MessageList;

/**
 * Sample used for interacting with Message Hub over Secure Kafka / Kafka Native
 * channels.
 *
 * @author IBM
 */
public class MessageHubJavaSample {

    private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
    private static final long HOUR_IN_MILLISECONDS = 3600000L;
    private static final Logger logger = Logger.getLogger(MessageHubJavaSample.class);
    private static String userDir, resourceDir;
    private static boolean isDistribution;
    private String topic="";
    private String kafkaHost = "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093,kafka02-prod01.messagehub.services.us-south.bluemix.net:9093,kafka03-prod01.messagehub.services.us-south.bluemix.net:9093,kafka04-prod01.messagehub.services.us-south.bluemix.net:9093,kafka05-prod01.messagehub.services.us-south.bluemix.net:9093";
    private String restHost = null;
    private String apiKey = null;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    public MessageHubJavaSample(){}
    
 
    public MessageHubJavaSample(String topicName){
    	 this.topic=topicName;
         String vcapServices = System.getenv("VCAP_SERVICES");
         ObjectMapper mapper = new ObjectMapper();
         System.out.println("vcapServices::" +vcapServices);
         System.setProperty("java.security.auth.login.config", "");
         if(vcapServices != null) {
             try {
                 // Parse VCAP_SERVICES into Jackson JsonNode, then map the 'messagehub' entry
                 // to an instance of MessageHubEnvironment.
                 JsonNode vcapServicesJson = mapper.readValue(vcapServices, JsonNode.class);
                 ObjectMapper envMapper = new ObjectMapper();
                 String vcapKey = null;
                 Iterator<String> it = vcapServicesJson.fieldNames();

                 // Find the Message Hub service bound to this application.
                 while (it.hasNext() && vcapKey == null) {
                     String potentialKey = it.next();

                     if (potentialKey.startsWith("messagehub")) {
                         logger.log(Level.INFO, "Using the '" + potentialKey + "' key from VCAP_SERVICES.");
                         vcapKey = potentialKey;
                     }
                 }

                 if (vcapKey == null) {
                     logger.log(Level.ERROR,
                                "Error while parsing VCAP_SERVICES: A Message Hub service instance is not bound to this application.");
                     return;
                 }
                 
                 MessageHubEnvironment messageHubEnvironment = envMapper.readValue(vcapServicesJson.get(vcapKey).get(0).toString(), MessageHubEnvironment.class);
                 MessageHubCredentials credentials = messageHubEnvironment.getCredentials();

                 kafkaHost = credentials.getKafkaBrokersSasl()[0];
                 restHost = credentials.getKafkaRestUrl();
                 apiKey = credentials.getApiKey();

                 System.out.println("kafkaHost::  "+kafkaHost + "restHost:: " +restHost + "apiKey:: " +apiKey);
                 
                 //updateJaasConfiguration(credentials);
             } catch(final Exception e) {
                 e.printStackTrace();
                 return;
             }
         }
     }
    
    //public static void main(String args[]) throws InterruptedException,
      public void InjestData(String data) throws InterruptedException,
            ExecutionException, IOException {
          String fieldName = "records";
          // Push a message into the list to be sent.
          MessageList list = new MessageList();
          list.push(data);
          System.out.println("++++++++++++data before publish:: " +list.toString());
          try {
        	  Properties clientConfig=getClientConfiguration(kafkaHost, apiKey, true);
              this.kafkaProducer = new KafkaProducer<byte[], byte[]>(clientConfig);
              System.out.println("++++++++topic::  "+topic);
              // Create a producer record which will be sent
              // to the Message Hub service, providing the topic
              // name, field name and message. The field name and
              // message are converted to UTF-8.
              ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
                  topic,
                  fieldName.getBytes("UTF-8"),
                  list.toString().getBytes("UTF-8"));

              System.out.println("Message Sent!!!");
              // Synchronously wait for a response from Message Hub / Kafka.
              RecordMetadata m = kafkaProducer.send(record).get();

              logger.log(Level.INFO, "Message produced, offset: " + m.offset());
          } catch (final Exception e) {
              e.printStackTrace();
              // Consumer will hang forever, so exit program.
              System.exit(-1);
          }
 
    }

 
 
    /**
     * Retrieve client configuration information, using a properties file, for
     * connecting to secure Kafka.
     *
     * @param broker
     *            {String} A string representing a list of brokers the producer
     *            can contact.
     * @param apiKey
     *            {String} The API key of the Bluemix Message Hub service.
     * @param isProducer
     *            {Boolean} Flag used to determine whether or not the
     *            configuration is for a producer.
     * @return {Properties} A properties object which stores the client
     *         configuration info.
     */
    public final Properties getClientConfiguration(String broker,
            String apiKey, boolean isProducer) {
        Properties props = new Properties();
        InputStream propsStream;
        String fileName= "resources/producer.properties";

        try {
        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        	propsStream = classLoader.getResourceAsStream(fileName);
            props.load(propsStream);
            propsStream.close();
        } catch (IOException e) {
            logger.log(Level.ERROR, "Could not load properties from file");
            return props;
        }

        System.out.println("+++++++++++++++++++++++++++bootstrap.servers:: " +broker);
        props.put("bootstrap.servers", broker);

        return props;
    }
}
