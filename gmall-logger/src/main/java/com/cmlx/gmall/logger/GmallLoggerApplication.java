package com.cmlx.gmall.logger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GmallLoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallLoggerApplication.class, args);
        System.out.println("日志采集系统启动成功");
    }

}
