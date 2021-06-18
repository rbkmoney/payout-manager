package com.rbkmoney.payout.manager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class PayoutManagerApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(PayoutManagerApplication.class, args);
    }

}
