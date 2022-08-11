package br.com.alura.ecommerce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "21,13,123123123";
        var email = "Bem vindo, estamos processando sua compra";
        var records = new ProducerRecord<>("LOJA_NOVO_PEDIDO", value, value);
        var emailRecord = new ProducerRecord<>("LOJA_ENVIAR_EMAIL", email, email);
        producer.send(records, getCallback()).get();
        producer.send(emailRecord, getCallback()).get();
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info("Mensagem enviada com sucesso: " + data.topic() + ":::partition " + data.partition()
                    + "/ offset " + data.offset() + "/timestamp " + data.timestamp());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
