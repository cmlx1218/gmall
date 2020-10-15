package com.cmlx.publisher.service;

import java.io.IOException;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/15 15:29
 */
public interface IPublisherService {

    /**
     * 查询日活总数
     * @param date
     * @return
     */
    Long getDauTotal(String date) throws IOException;



}
