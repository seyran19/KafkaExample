package com.email.notification.handler;


import com.email.notification.core.event.ProductCreatedEvent;
import com.email.notification.exception.NonRetryableException;
import com.email.notification.exception.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;


@Component
@KafkaListener(topics = "product-created-events-topic") // groupId = "product-created-events" можно и так указать groupId
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private final RestTemplate RESTTEMPLATE;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.RESTTEMPLATE = restTemplate;
    }

    //     если в топикке прочитает объект ProductDeletedEvent то вызовет этот метод
//    @KafkaListener
//    public void handle(ProductDeletedEvent productDeletedEvent){
//
//    }

//  если в топикке прочитает объект ProductCreatedEvent то вызовет этот метод
    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){

        LOGGER.info("Received event: {}", productCreatedEvent.getTitle());
        String url = "http://localhost:8090/response/200";
        try {

            ResponseEntity<String> response = RESTTEMPLATE.exchange(url, HttpMethod.GET, null, String.class);//null - отправляем, String - получаем
            if (response.getStatusCode().value() == HttpStatus.OK.value()){
                LOGGER.info("Received response: {}", response.getBody());

            }
        }catch (ResourceAccessException e){
            LOGGER.error(e.getMessage());
            throw new RetryableException(e);
        } catch (Exception e){
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }
    }

}
