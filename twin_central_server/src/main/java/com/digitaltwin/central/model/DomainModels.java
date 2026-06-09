package com.digitaltwin.central.model;

import jakarta.persistence.*;
import lombok.Data;
import java.time.ZonedDateTime;

public class DomainModels {

    @Entity @Table(name = "stages") @Data
    public static class Stage {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
        private String name;
        private Integer capacity;
    }

    @Entity @Table(name = "webhooks") @Data
    public static class Webhook {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
        private String url;
        private String secret;
        private String clientType;
    }

    @Entity @Table(name = "alerts") @Data
    public static class Alert {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
        private Long stageId;
        private String type;
        private String message;
        private String severity;
        private ZonedDateTime createdAt;
        private Boolean resolved = false;
        private ZonedDateTime resolvedAt;
    }
}