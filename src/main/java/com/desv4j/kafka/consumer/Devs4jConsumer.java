package com.desv4j.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Devs4jConsumer {
	
	private static final Logger log = LoggerFactory.getLogger( Devs4jConsumer.class );
	
	public static void main( String[] args ) {
		Properties props = new Properties();
		
		props.setProperty( "bootstrap.servers", "localhost:9092" );
		props.setProperty( "group.id", "devs4j-group" );
		props.setProperty( "enable.auto.commit", "true" );
		props.setProperty( "auto.commit.interval.ms", "1000" );
		props.setProperty( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
		props.setProperty( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
		
		try ( KafkaConsumer<String, String> consumer = new KafkaConsumer<>( props ); ) {
			// Nos suscribimos a un topic, pueden ser a varios
			// consumer.subscribe( Arrays.asList( "devs4j-topic" ) );
			consumer.subscribe( Collections.singletonList( "devs4j-topic" ) );
			
			while ( true ) {
				// Obtenemos los registros cada 100 milisegundos
				ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 100 ) );
				// Iteramos los registros recuperados
				for ( ConsumerRecord<String, String> record : records )
					log.info(
						"offset = {}, Partition = {}, key = {}, value = {}",
						record.offset(), record.partition(), record.key(), record.value()
					);
			}
		}
	}
}
