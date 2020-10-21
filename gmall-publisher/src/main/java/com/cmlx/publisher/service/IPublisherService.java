package com.cmlx.publisher.service;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/15 15:29
 */
public interface IPublisherService {

    /**
     * 查询日活总数
     *
     * @param date
     * @return
     */
    Long getDauTotal(String date) throws IOException;

    /**
     * 查询小时日活数以及跟前一天的对比
     *
     * @param date
     * @return
     * @throws IOException
     */
    Map getDauHourMap(String date) throws IOException;

    /**
     * 获取订单总金额数
     *
     * @param date
     * @return
     */
    Double getOrderAmount(String date) throws IOException;

    /**
     * 获取订单小时总金额数以及和前一天的对比
     *
     * @param date
     * @return
     */
    Map getOrderAmountHourMap(String date) throws IOException;

    /**
     * @param date
     * @param keyword
     * @param pageNum
     * @param pageSize
     * @return
     */
    Map getSaleDetailMap(String date, String keyword, Integer pageNum, Integer pageSize, String aggsFieldName, Integer aggsSize) throws IOException;


}
