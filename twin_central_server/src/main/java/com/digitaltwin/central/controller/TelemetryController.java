package com.digitaltwin.central.controller;

import com.digitaltwin.central.model.DomainModels.*;
import com.digitaltwin.central.repository.Repositories.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.time.ZonedDateTime;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class TelemetryController {

    private final StageRepository stageRepo;
    private final AlertRepository alertRepo;

    @PostMapping("/telemetry")
    public ResponseEntity<String> receiveTelemetry(@RequestBody TelemetryRequest request) {
        Stage stage = stageRepo.findById(request.getStageId()).orElseThrow();

        if (request.getCurrentCrowd() >= stage.getCapacity()) {
            Alert alert = new Alert();
            alert.setStageId(stage.getId());
            alert.setType("OVER_CROWD");
            alert.setMessage("Stage " + stage.getName() + " is overcrowded: " + request.getCurrentCrowd() + "/" + stage.getCapacity());
            alert.setSeverity("HIGH");
            alert.setCreatedAt(ZonedDateTime.now());
            alertRepo.save(alert);
            
            System.out.println("🚨 ALERT TRIGGERED: " + alert.getMessage());
            return ResponseEntity.ok("Alert generated.");
        }
        return ResponseEntity.ok("Telemetry accepted. Status normal.");
    }
}

@Data
class TelemetryRequest {
    private Long stageId;
    private Integer currentCrowd;
}