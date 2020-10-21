package com.cmlx.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.cmlx.publisher.bean.Option;
import com.cmlx.publisher.bean.OptionGroup;
import com.cmlx.publisher.bean.SaleDetailInfo;
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
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = iPublisherService.getDauTotal(date);
        dauMap.put("value", 2333);
        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "newMid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 2333);
        totalList.add(newMidMap);

        Map dateTotalAmount = new HashMap();
        newMidMap.put("id", "totalAmount");
        newMidMap.put("name", "总交易额");
        Double orderAmount = iPublisherService.getOrderAmount(date);
        newMidMap.put("value", orderAmount);
        totalList.add(dateTotalAmount);

        return JSON.toJSONString(totalList);

    }

    @RequestMapping("realtime-hour")
    public String getHourTotal(String id, String today) throws IOException {

        Map hourMap = new HashMap();
        if ("dau".equals(id)) {
            Map dauHourTDMap = iPublisherService.getDauHourMap(today);
            Map dauHourYDMap = iPublisherService.getDauHourMap(DateUtil.getYesterday(today));
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);
        } else if ("totalAmount".equals(id)) {
            Map dauHourTDMap = iPublisherService.getOrderAmountHourMap(today);
            Map dauHourYDMap = iPublisherService.getOrderAmountHourMap(DateUtil.getYesterday(today));
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);
        }
        return JSON.toJSONString(hourMap);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(String date, Integer startPage, Integer size, String keyword) throws IOException {
        Map saleDetailMapWithGender = iPublisherService.getSaleDetailMap(date, keyword, startPage, size, "user_gender", 2);
        Integer total = (Integer) saleDetailMapWithGender.get("total");
        List<Map> detailList = (List<Map>) saleDetailMapWithGender.get("detail");
        Map aggsMapGender = (Map) saleDetailMapWithGender.get("aggsMap");

        Long femaleCount = (Long) aggsMapGender.getOrDefault("F", 0);
        Long maleCount = (Long) aggsMapGender.getOrDefault("M", 0);

        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        List<Option> optionListGender = new ArrayList<>();
        optionListGender.add(new Option("男", maleRatio));
        optionListGender.add(new Option("女", femaleRatio));

        OptionGroup optionGroupGender = new OptionGroup("性别占比", optionListGender);

        Map saleDetailMapWithAge = iPublisherService.getSaleDetailMap(date, keyword, startPage, size, "user_age", 100);
        Map aggsMapAge = (Map) saleDetailMapWithAge.get("aggsMap");

        Long age_20count = 0L;
        Long age20_30count = 0L;
        Long age30_count = 0L;

        for (Object o : aggsMapAge.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String age = (String) entry.getKey();
            Long ageCount = (Long) entry.getValue();
            if (Integer.parseInt(age) < 20) {
                age_20count += ageCount;
            } else if (Integer.parseInt(age) >= 20 && Integer.parseInt(age) <= 30) {
                age20_30count += ageCount;
            } else {
                age30_count += ageCount;
            }
        }
        //各年龄段的占比
        Double age_20Ratio = Math.round(age_20count * 1000D / total) / 10D;
        Double age20_30Ratio = Math.round(age20_30count * 1000D / total) / 10D;
        Double age30_Ratio = Math.round(age30_count * 1000D / total) / 10D;

        List<Option> optionListAge = new ArrayList<>();
        optionListAge.add(new Option("20岁以下", age_20Ratio));
        optionListAge.add(new Option("20岁-30岁", age20_30Ratio));
        optionListAge.add(new Option("30岁以上", age30_Ratio));
        OptionGroup optionGroupAge = new OptionGroup("年龄占比", optionListAge);

        List<OptionGroup> optionGroups = new ArrayList<>();
        optionGroups.add(optionGroupGender);
        optionGroups.add(optionGroupAge);

        SaleDetailInfo saleDetailInfo = new SaleDetailInfo(total, optionGroups, detailList);

        return JSON.toJSONString(saleDetailInfo);
    }


}














