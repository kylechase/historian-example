package com.inductiveautomation.ignition.examples.historian;

import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.DefaultValue;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.Description;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.DescriptionKey;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.FormCategory;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.FormField;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.Label;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.Required;
import com.inductiveautomation.ignition.gateway.web.nav.FormFieldType;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration for a Example Historian provider.
 */
public record ExampleHistorianSettings(
        @FormCategory("GENERAL")
        @Label("Webdev Endpoint")
        @FormField(FormFieldType.TEXT)
        @DefaultValue("http://localhost:8088/system/webdev/test/historian")
        @Required
        @DescriptionKey("ExampleHistorianSettings.WebdevEndpoint.Desc")
        String webdevEndpoint

//        ,@FormCategory("CUSTOM SETTINGS")
//        @Label("Password")
//        @FormField(FormFieldType.SECRET)
//        @DescriptionKey("SecretResource.password.Desc")
//        @Description(".")
//        SecretConfig password
) implements HistorianSettings {

    public static final ExampleHistorianSettings DEFAULT =
        new ExampleHistorianSettings("http://localhost:8088/system/webdev/test/historian");

    /**
     * Canonical constructor that fills in default values for any null or blank parameters.
     *
     * @param webdevEndpoint   The webdev endpoint to use
     */
    public ExampleHistorianSettings {
      webdevEndpoint = StringUtils.defaultIfBlank(webdevEndpoint, DEFAULT.webdevEndpoint());
    }
}
