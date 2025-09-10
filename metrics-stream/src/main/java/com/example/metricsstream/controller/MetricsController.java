package com.example.metricsstream.web;

import com.example.metricsstream.repository.MetricsReadRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
}
