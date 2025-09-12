package com.example.metricsstream.controller;

import com.example.metricsstream.repository.MetricsReadRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final MetricsReadRepository repo;

    @GetMapping("sla")
    public List<MetricsReadRepository.SlaRow> getSla(
            @RequestParam(name = "city", required = false) String city,
            @RequestParam(name = "limit", defaultValue = "50") int limit
    ) {
        return repo.findSla(city, limit);
    }

    @GetMapping("delivery-time")
    public List<MetricsReadRepository.TimeRow> getDeliveryTime(
            @RequestParam(name = "city", required = false) String city,
            @RequestParam(name = "limit", defaultValue = "50") int limit
    ) {
        return repo.findDeliveryTime(city, limit);
    }

    @GetMapping("alerts")
    public List<MetricsReadRepository.AlertRow> getAlerts(
            @RequestParam(name = "limit", defaultValue = "50") int limit
    ) {
        return repo.findRecentAlerts(limit);
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "metrics-stream");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleError(Exception e) {
        log.error("Metrics API error", e);
        return ResponseEntity.status(500).body("Internal error");
    }
}
