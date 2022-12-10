package com.desv4j.kafka.multithread;

import com.desv4j.kafka.consumer.Devs4jConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class Devs4jThreadConsumer extends Thread {
	private final KafkaConsumer<String, String> consumer;
	
	private static final Logger        log    = LoggerFactory.getLogger( Devs4jThreadConsumer.class );
	private final        AtomicBoolean closed = new AtomicBoolean( false );
	
	public Devs4jThreadConsumer( KafkaConsumer<String, String> consumer ) {
		this.consumer = consumer;
	}
	
	@Override
	public void run() {
		consumer.subscribe( Collections.singletonList( "devs4j-topic" ) );
		
		try {
			while ( !closed.get() ) {
				// Obtenemos los registros cada 100 milisegundos
				ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 100 ) );
				// Iteramos los registros recuperados
				for ( ConsumerRecord<String, String> record : records ) {
					log.debug(
						"offset = {}, Partition = {}, key = {}, value = {}",
						record.offset(), record.partition(), record.key(), record.value()
					);
					if ( Integer.parseInt( record.key() ) % 100_000 == 0 ) {
						log.info(
							"offset = {}, Partition = {}, key = {}, value = {}",
							record.offset(), record.partition(), record.key(), record.value()
						);
					}
				}
			}
		}
		catch ( WakeupException e ) {
			if ( !closed.get() )
				throw e;
		}
		finally {
			consumer.close();
		}
	}
	
	public void shutdown() {
		closed.set( true );
		consumer.wakeup();
	}
}
