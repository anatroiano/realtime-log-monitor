package com.example.log_collector_service.http.contrroller;

import com.example.log_collector_service.application.LogIngestionService;
import com.example.log_collector_service.domain.LogEvent;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Tag(name = "Logs", description = "Operations related to application log ingestion")
@RestController
public class LogController {
    private final LogIngestionService ingestion;

    public LogController(LogIngestionService ingestion) {
        this.ingestion = ingestion;
    }

    @Operation(summary = "Submit a log batch", description = "Receives application logs and publishes them to Kafka")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Log batch accepted successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input data"),
            @ApiResponse(responseCode = "413", description = "Request body too large"),
            @ApiResponse(responseCode = "503", description = "Log batch publication could not be confirmed")
    })
    @PostMapping(path = "/logs")
    public CompletableFuture<ResponseEntity<Void>> ingest(@RequestBody @NotEmpty @Size(max = 1000) List<@NotNull @Valid LogEvent> events) {
        return ingestion.ingest(events)
                .thenApply(ignored -> ResponseEntity.accepted().build());
    }
}
