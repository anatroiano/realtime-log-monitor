package com.example.alert_service.controller;

import com.example.alert_service.dto.AlertResponse;
import com.example.alert_service.service.AlertService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Alerts", description = "Incident history produced from aggregated logs")
@RestController
@RequiredArgsConstructor
public class AlertController {
    private final AlertService alertService;

    @Operation(summary = "List alerts", description = "Returns a paginated list of alerts")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Alert page returned"),
            @ApiResponse(responseCode = "400", description = "Invalid filter or sort parameters")
    })
    @GetMapping("/alerts")
    public ResponseEntity<Page<AlertResponse>> findHistory(
            @RequestParam(required = false) @Size(min = 1, max = 128) String serviceName,
            @ParameterObject Pageable pageable) {
        return ResponseEntity.ok(alertService.findHistory(serviceName, pageable).map(AlertResponse::from));
    }
}
