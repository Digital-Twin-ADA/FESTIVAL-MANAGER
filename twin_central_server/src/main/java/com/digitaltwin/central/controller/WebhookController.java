package com.digitaltwin.central.controller;

import com.digitaltwin.central.model.DomainModels.Webhook;
import com.digitaltwin.central.repository.Repositories.WebhookRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/webhooks")
@RequiredArgsConstructor
public class WebhookController {

    private final WebhookRepository repo;

    @PostMapping
    public ResponseEntity<Webhook> registerWebhook(@RequestBody Webhook webhook) {
        return ResponseEntity.ok(repo.save(webhook));
    }

    @GetMapping
    public ResponseEntity<List<Webhook>> listWebhooks() {
        return ResponseEntity.ok(repo.findAll());
    }
}