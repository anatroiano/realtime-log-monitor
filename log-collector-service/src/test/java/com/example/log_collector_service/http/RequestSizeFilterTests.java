package com.example.log_collector_service.http;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class RequestSizeFilterTests {
    @Test
    void rejectsOversizedBodiesWithoutContentLengthBeforeInvokingTheController() throws Exception {
        var request = new MockHttpServletRequest() {
            @Override
            public long getContentLengthLong() {
                return -1;
            }
        };
        request.setMethod("POST");
        request.setServletPath("/logs");
        request.setContent(new byte[1025]);
        var response = new MockHttpServletResponse();
        var invoked = new AtomicBoolean();

        var mapper = JsonMapper.builder().build();
        new RequestSizeFilter(1024, mapper).doFilter(request, response, (req, res) -> invoked.set(true));

        assertThat(response.getStatus()).isEqualTo(413);
        assertThat(response.getContentType()).isEqualTo("application/problem+json");
        var problem = mapper.readTree(response.getContentAsString());
        assertThat(problem.get("status").asInt()).isEqualTo(413);
        assertThat(problem.get("title").asString()).isEqualTo("Content Too Large");
        assertThat(problem.get("detail").asString()).isEqualTo("The log batch exceeds the request size limit.");
        assertThat(invoked).isFalse();
    }
}
