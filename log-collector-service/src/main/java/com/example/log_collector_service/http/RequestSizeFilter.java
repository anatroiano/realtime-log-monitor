package com.example.log_collector_service.http;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@Component
public class RequestSizeFilter extends OncePerRequestFilter {
    private final int maxBytes;
    private final JsonMapper mapper;

    public RequestSizeFilter(@Value("${collector.max-request-bytes}") int maxBytes, JsonMapper mapper) {
        if (maxBytes < 1 || maxBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("collector.max-request-bytes must be between 1 and 2147483646");
        }
        this.maxBytes = maxBytes;
        this.mapper = mapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equals(request.getMethod()) || !"/logs".equals(request.getServletPath());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain) throws ServletException, IOException {
        byte[] body = request.getContentLengthLong() > maxBytes
                ? null : request.getInputStream().readNBytes(maxBytes + 1);
        if (body == null || body.length > maxBytes) {
            ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                    HttpStatus.CONTENT_TOO_LARGE, "The log batch exceeds the request size limit.");
            response.setStatus(problem.getStatus());
            response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
            mapper.writeValue(response.getOutputStream(), problem);
            return;
        }
        chain.doFilter(new HttpServletRequestWrapper(request) {
            @Override
            public ServletInputStream getInputStream() {
                ByteArrayInputStream input = new ByteArrayInputStream(body);
                return new ServletInputStream() {
                    @Override
                    public int read() {
                        return input.read();
                    }

                    @Override
                    public boolean isFinished() {
                        return input.available() == 0;
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setReadListener(ReadListener listener) {
                        throw new UnsupportedOperationException("Non-blocking request reads are not supported");
                    }
                };
            }
        }, response);
    }
}
