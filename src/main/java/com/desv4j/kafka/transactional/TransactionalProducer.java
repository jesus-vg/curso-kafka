package com.desv4j.kafka.transactional;

import com.desv4j.kafka.producer.Devs4jProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TransactionalProducer {
	
	private static Logger log = LoggerFactory.getLogger( Devs4jProducer.class );
	
	public static void main( String[] args ) {
		long       startTime = System.currentTimeMillis();
		Properties props     = new Properties();
		
		// Broker de kafka al que nos vamos a conectar
		props.put( "bootstrap.servers", "localhost:9092" );
		// confirmation de recibido, 0 no, 1 si se recibió de al menos un broker, all de todos los brokers
		props.put( "acks", "all" );
		props.put( "transactional.id", "devs4js-producer-id" );
		// Indicamos el tipo de dato que tendrá la key
		props.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
		// Indicamos el tipo de dato que tendrá el value
		props.put( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
		props.put( "linger.ms", "10" );
		
		// Hacemos este tray e implementamos código dentro de los paréntesis para auto invocar el método close.
		try ( Producer<String, String> producer = new KafkaProducer<String, String>( props ) ) {
			try {
				// preparamos la transaccion
				producer.initTransactions();
				producer.beginTransaction();
				
				for ( int i = 0; i < 100_000; i++ ) {
					// Con esta linea enviamos mensajes
					producer.send( new ProducerRecord<>( "devs4j-topic", i + "", "message " + ( i + 1 ) ) );
					
					// Si dejamos que ocurra un error, esto pasaría al catch donde se indica que aborte los mensajes
					// si pasa eso, en la consola nunca se visualizaran los mensajes enviados.
					if ( i == 50_000 ) {
						throw new Exception( "Unexpected Exception" );
					}
				}
				
				producer.commitTransaction();
				producer.flush();
			}
			catch ( Exception e ) {
				log.error( "Error", e );
				producer.abortTransaction();
				
			}
			
		}
		
		log.info( "Processing time = {} ms", ( System.currentTimeMillis() - startTime ) );
	}
}
