package br.com.itau.exemplo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;

@Component
@org.springframework.kafka.annotation.KafkaListener(topics = "${consumer.topico}",groupId = "${spring.kafka.consumer.group-id}")
public class KafkaListener {

    @KafkaHandler
    public void consume(@Payload String message
            , @Header(KafkaHeaders.OFFSET) int offset
            , @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key
            , @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition
            , @Header(KafkaHeaders.RECEIVED_TIMESTAMP) String timestamp)
    {
        System.out.println("Mensagem : "+message+
                " | Key : "+key+
                " | Partition : "+partition+
                " | Offset : "+offset+
                " | Timestamp : "+ Instant.ofEpochMilli(Long.parseLong(timestamp)).toString());
    }
}
