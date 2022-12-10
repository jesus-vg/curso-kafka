package com.desv4j.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Devs4jProducer {
	
	private static Logger log = LoggerFactory.getLogger( Devs4jProducer.class );
	
	public static void main( String[] args ) {
		long       startTime = System.currentTimeMillis();
		Properties props     = new Properties();
		
		// Broker de kafka al que nos vamos a conectar
		props.put( "bootstrap.servers", "localhost:9092" );
		// confirmation de recibido, 0 no, 1 si se recibió de al menos un broker, all de todos los brokers
		props.put( "acks", "1" );
		// Indicamos el tipo de dato que tendrá la key
		props.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
		// Indicamos el tipo de dato que tendrá el value
		props.put( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
		props.put( "linger.ms", "10" );
		
		// Hacemos este try e implementamos código dentro de los paréntesis para auto invocar el método close.
		try ( Producer<String, String> producer = new KafkaProducer<String, String>( props ) ) {
			for ( int i = 0; i < 100; i++ ) {
				// Con esta linea enviamos mensajes
				/*producer.send(
					new ProducerRecord<>( "devs4j-topic", i + "", "message " + ( i + 1 ) )
				);*/
				producer.send(
					new ProducerRecord<>( "devs4j-topic", ( i % 2 == 0 ) ? "key-2" : "key-3", "message " + ( i + 1 ) )
				);
				
				// forma asíncrona
				// producer.send(
				// 	new ProducerRecord<>( "devs4j-topic", "key-" + i, "message " + (i + 1) )
				// ).get();
			}
			producer.flush();
		}
		
		// 2,682 ms - con linger.ms de 6
		// 2,885 ms - con linger.ms de 10
		
		log.info( "Processing time = {} ms", ( System.currentTimeMillis() - startTime ) );
	}
}
