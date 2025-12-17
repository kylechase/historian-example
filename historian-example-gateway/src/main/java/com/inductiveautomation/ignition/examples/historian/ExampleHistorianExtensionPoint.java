package com.inductiveautomation.ignition.examples.historian;

import com.inductiveautomation.historian.gateway.api.Historian;
import com.inductiveautomation.historian.gateway.api.HistorianProvider;
import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.ignition.gateway.config.DecodedResource;
import com.inductiveautomation.ignition.gateway.config.ExtensionPointConfig;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.SchemaUtil;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.web.nav.ExtensionPointResourceForm;
import com.inductiveautomation.ignition.gateway.web.nav.WebUiComponent;

import com.inductiveautomation.historian.gateway.api.HistorianExtensionPoint;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

public class ExampleHistorianExtensionPoint extends HistorianExtensionPoint<ExampleHistorianSettings> {

    public static final String EXTENSION_POINT_TYPE = "HistorianExample";

    public ExampleHistorianExtensionPoint() {
        super(
                EXTENSION_POINT_TYPE,
            "ExampleHistorianSettings.HistorianExampleType.Name",
            "ExampleHistorianSettings.HistorianExampleType.Desc");
    }

    @Override
    public Optional<WebUiComponent> getWebUiComponent(ComponentType type) {
        return Optional.of(new ExtensionPointResourceForm(
            HistorianExtensionPoint.getResourceType(),
            "Historian",
                EXTENSION_POINT_TYPE,
            SchemaUtil.fromType(HistorianProvider.class),
            SchemaUtil.fromType(ExampleHistorianSettings.class)));
    }

    @NotNull
    @Override
    public Historian<ExampleHistorianSettings> createHistorianProvider(
        @NotNull GatewayContext context,
        DecodedResource<ExtensionPointConfig<HistorianProvider, HistorianSettings>> decodedResource
    ) {
        var settings = getSettings(decodedResource.config())
            .orElse(ExampleHistorianSettings.DEFAULT);

        return new ExampleHistorianProvider(context, decodedResource.name(), settings);
    }

    @Override
    public Optional<Class<ExampleHistorianSettings>> settingsType() {
        return Optional.of(ExampleHistorianSettings.class);
    }
}
