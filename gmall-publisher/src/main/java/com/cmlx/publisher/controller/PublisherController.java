package com.cmlx.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.cmlx.publisher.service.IPublisherService;
import com.cmlx.publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/15 15:29
 */
@RestController
public class PublisherController {

    @Autowired
    IPublisherService iPublisherService;

    @GetMapping("/getTotal")
    public String getTotal(String date) throws IOException {
        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = iPublisherService.getDauTotal(date);
        dauMap.put("value",2333);
        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id","newMid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",2333);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);

    }

    @RequestMapping("realtime-hour")
    public String getHourTotal(String id,String today) throws IOException {
        Map dauHourTDMap = iPublisherService.getDauHourMap(today);
        Map dauHourYDMap = iPublisherService.getDauHourMap(DateUtil.getYesterday(today));

        Map hourMap = new HashMap();
        hourMap.put("today",dauHourTDMap);
        hourMap.put("yesterday",dauHourYDMap);

        return JSON.toJSONString(hourMap);
    }


}
