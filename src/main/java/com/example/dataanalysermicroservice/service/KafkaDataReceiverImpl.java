package com.example.dataanalysermicroservice.service;

import com.example.dataanalysermicroservice.config.LocalDateTimeDeserializer;
import com.example.dataanalysermicroservice.model.Data;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class KafkaDataReceiverImpl implements KafkaDataReceiver {

    private final KafkaReceiver<String, Object> receiver;
    private final LocalDateTimeDeserializer localDateTimeDeserializer;
    private final KafkaDataService kafkaDataService;

    /**
     * После создания бина данного класса запустится этот метод
     */
    @PostConstruct
    private void init() {
        fetch();
    }

    @Override
    public void fetch() {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, localDateTimeDeserializer)
                .create();
        receiver.receive() //подписываемся на событие
                .subscribe(r -> { //после того, как отработало событие и нам пришел ответ -> обрабатываем сообщение
                    Data data = gson
                            .fromJson(r.value().toString(), Data.class);
                    kafkaDataService.handle(data);

                    // Этим мы говорим kafka, что это сообщение мы получили, обработали и можно присылать следующее сообщение
                    // Без этого мы будем читать одно и тоже собщение при каждом запуске приложения, т.к. kafka будет думать,
                    // что мы его не обработали, что что-то пошло не так и нам необходмо прочитать его еще раз
                    r.receiverOffset().acknowledge();
                });
    }
}
