package com.digitaltwin.central.repository;

import com.digitaltwin.central.model.DomainModels.*;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Repositories {
    interface StageRepository extends JpaRepository<Stage, Long> {}
    interface WebhookRepository extends JpaRepository<Webhook, Long> {}
    interface AlertRepository extends JpaRepository<Alert, Long> {}
}