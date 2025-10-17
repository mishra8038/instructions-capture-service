package com.example.instructions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class InstructionsCaptureApplication {
    public static void main(String[] args) {
        SpringApplication.run(InstructionsCaptureApplication.class, args);
    }
}