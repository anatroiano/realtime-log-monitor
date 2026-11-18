package com.example.alert_service.controller;

import com.example.alert_service.domain.Alert;
import com.example.alert_service.service.AlertService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.core.PropertyReferenceException;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AlertController.class)
class AlertControllerTests {
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final Pageable DEFAULT_PAGE = PageRequest.of(0, 20);

    @Autowired
    MockMvc mvc;
    @MockitoBean
    AlertService alertService;

    @Test
    void returnsAlertsWithDefaultPaging() throws Exception {
        var alert = new Alert(UUID.fromString("6f1c2a52-6a4e-3d39-9b55-0c9f1b8f7a10"), "checkout",
                "errors-per-minute-over-50", START, START.plusSeconds(60), 51, START.plusSeconds(61));
        given(alertService.findHistory(isNull(), eq(DEFAULT_PAGE)))
                .willReturn(new PageImpl<>(List.of(alert), DEFAULT_PAGE, 1));

        mvc.perform(get("/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].alertId").value("6f1c2a52-6a4e-3d39-9b55-0c9f1b8f7a10"))
                .andExpect(jsonPath("$.content[0].serviceName").value("checkout"))
                .andExpect(jsonPath("$.content[0].windowStart").value("2026-11-11T12:00:00Z"))
                .andExpect(jsonPath("$.content[0].errorCount").value(51))
                .andExpect(jsonPath("$.totalElements").value(1));
    }

    @Test
    void filtersByServiceName() throws Exception {
        given(alertService.findHistory(eq("checkout"), any()))
                .willReturn(new PageImpl<>(List.of(), DEFAULT_PAGE, 0));

        mvc.perform(get("/alerts").param("serviceName", "checkout")).andExpect(status().isOk());

        then(alertService).should().findHistory("checkout", DEFAULT_PAGE);
    }

    @Test
    void appliesRequestedPageSizeAndSort() throws Exception {
        var requestedPage = PageRequest.of(1, 10, Sort.by(Sort.Direction.ASC, "errorCount"));
        given(alertService.findHistory(isNull(), eq(requestedPage)))
                .willReturn(new PageImpl<>(List.of(), requestedPage, 0));

        mvc.perform(get("/alerts").param("page", "1").param("size", "10").param("sort", "errorCount,asc"))
                .andExpect(status().isOk());

        then(alertService).should().findHistory(null, requestedPage);
    }

    @Test
    void rejectsBlankOrOversizedServiceName() throws Exception {
        mvc.perform(get("/alerts").param("serviceName", "")).andExpect(status().isBadRequest());
        mvc.perform(get("/alerts").param("serviceName", "a".repeat(129))).andExpect(status().isBadRequest());

        then(alertService).should(never()).findHistory(any(), any());
    }

    @Test
    void rejectsSortingByUnknownProperty() throws Exception {
        given(alertService.findHistory(any(), any())).willThrow(
                new PropertyReferenceException("unknown", TypeInformation.of(Alert.class), List.of()));

        mvc.perform(get("/alerts").param("sort", "unknown"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").exists());
    }
}
