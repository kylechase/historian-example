package com.inductiveautomation.ignition.examples.historian;

import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.DescriptionKey;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.FormCategory;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.FormField;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.Label;
import com.inductiveautomation.ignition.gateway.web.nav.FormFieldType;

/**
 * Configuration for an Example Historian provider.
 */
public record ExampleHistorianSettings(
        @FormCategory("General")
        @Label("Maximum Stored datapoints")
        @FormField(FormFieldType.NUMBER)
        @DescriptionKey("ExampleHistorianSettings.MaxDataPoints.Desc")
        Integer maxDatapoints,
        @FormCategory("General")
        @Label("Maximum age of datapoints in milliseconds")
        @FormField(FormFieldType.NUMBER)
        @DescriptionKey("ExampleHistorianSettings.MaxAge.Desc")
        Long maxAge

) implements HistorianSettings {

    public static final ExampleHistorianSettings DEFAULT =
        new ExampleHistorianSettings(10000, 86400000L);

    /**
     * Canonical constructor that fills in default values for any null or blank parameters.
     *
     * @param maxDatapoints   The max points to store
     * @param maxAge The max age of records on millis
     */
    public ExampleHistorianSettings {
      maxDatapoints = maxDatapoints == null ? DEFAULT.maxDatapoints : maxDatapoints;
      maxAge = maxAge == null ? DEFAULT.maxAge : maxAge;
    }
}
