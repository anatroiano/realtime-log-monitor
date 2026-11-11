package com.example.log_collector_service.http.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler({
            HttpMessageNotReadableException.class,
            MethodArgumentNotValidException.class,
            HandlerMethodValidationException.class
    })
    ProblemDetail invalidBatch(Exception exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Provide a non-empty JSON array of at most 1000 valid log events.");
    }

    @ExceptionHandler(LogPublicationException.class)
    ResponseEntity<ProblemDetail> publicationFailed(LogPublicationException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Batch publication could not be confirmed. Retry the entire batch with the same logId values."
        );

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "1").body(problem);
    }

}
