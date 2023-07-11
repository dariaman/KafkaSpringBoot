package com.msgbroker.kafkatest;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
@AllArgsConstructor
public class KafkaController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Gson jsonConverter;

    @PostMapping
    public void post(@RequestBody String simplemodel) {
        kafkaTemplate.send("myTopic", simplemodel);
    }

    @KafkaListener(topics = "myTopic")
    public void getFromKasfka(String simpleModel) {
        System.out.println(simpleModel);

        Model1 model1 = (Model1)jsonConverter.fromJson(simpleModel,Model1.class);
        System.out.println(model1.toString());

    }

    @PostMapping("v2")
    public  void post(@RequestBody Model2 model2){
        kafkaTemplate.send("topic2",jsonConverter.toJson(model2));
    }

    @KafkaListener(topics = "topic2")
    public void getFromKasfka2(String simpleModel) {
        System.out.println(simpleModel);

        Model2 model2 = (Model2)jsonConverter.fromJson(simpleModel,Model2.class);
        System.out.println(model2.toString());

    }


}
