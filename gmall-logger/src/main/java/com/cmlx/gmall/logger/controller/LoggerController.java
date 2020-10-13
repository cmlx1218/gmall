package com.cmlx.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cmlx.gmall.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/12 14:51
 */
@RestController //等同于Controller + ResponseBody
public class LoggerController {

    private static final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("/log")
    @ResponseBody
    public String doLog(String log) {

        //补时间戳
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("createTime", System.currentTimeMillis());
        //落盘到log文件中  log4j
        logger.info(jsonObject.toJSONString());
        //发送kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonObject.toJSONString());
        }

        // 服务器上执行 bin/kafka-console-consumer.sh --bootstrap-server master:6667,slave1:6667,slave2:6667 --topic GMALL_STARTUP --from-beginning   来消费消息进行测试

        //System.out.println(log);
        return "success";
    }

}
