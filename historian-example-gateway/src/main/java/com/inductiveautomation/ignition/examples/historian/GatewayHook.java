package com.inductiveautomation.ignition.examples.historian;

import com.inductiveautomation.ignition.common.licensing.LicenseState;
import com.inductiveautomation.ignition.common.util.LoggerEx;
import com.inductiveautomation.ignition.gateway.config.ExtensionPoint;
import com.inductiveautomation.ignition.gateway.config.migration.ExtensionPointRecordMigrationStrategy;
import com.inductiveautomation.ignition.gateway.config.migration.IdbMigrationStrategy;
import com.inductiveautomation.ignition.gateway.config.migration.NamedRecordMigrationStrategy;
import com.inductiveautomation.ignition.gateway.config.migration.SingletonRecordMigrationStrategy;
import com.inductiveautomation.ignition.gateway.model.AbstractGatewayModuleHook;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import java.util.List;

/**
 * The GatewayHook is the main entry point for a Gateway module. It is instantiated very early in the Gateway startup
 * process, before most other services are available. It is responsible for registering extension points, migration
 * strategies, and other module-level functionality.
 */
public class GatewayHook extends AbstractGatewayModuleHook {
    private static final LoggerEx LOGGER = LoggerEx.newBuilder()
        .build("example.historian.GatewayHook");

    @Override
    public void setup(GatewayContext context) {
        LOGGER.info("Setting up example historian");
    }

    @Override
    public void startup(LicenseState licenseState) {
        LOGGER.info("Starting up example historian");
    }

    @Override
    public void shutdown() {
        LOGGER.info("Shutting down example historian");
    }

    /**
     * Here we tell the configuration management system about our "migration strategy", which adapts the legacy <=8.1
     * "PersistentRecord" storage to our new configuration management approach. You can use one of the existing builders
     * here, such as {@link ExtensionPointRecordMigrationStrategy}, {@link NamedRecordMigrationStrategy},
     * or {@link SingletonRecordMigrationStrategy}, or implement your own entirely via the {@link IdbMigrationStrategy}
     * interface.
     */
    @Override
    public List<IdbMigrationStrategy> getRecordMigrationStrategies() {
        // This sample wasn't available for <=8.1, so we don't need any migration strategies as there are no records
        // to migrate.
        //
        // In fact, we could just omit this method entirely, as the default implementation returns an empty list.
        // However, we include it here for demonstration purposes.
        return List.of();
    }

    /**
     * In a change from the <=8.1 model, any and all extension points, no matter <b>which</b> extension point they
     * extend, must be declared in your GatewayHook. They will be separated by the gateway and the appropriate lifecycle
     * management will be handled for you.
     */
    @Override
    public List<ExtensionPoint<?>> getExtensionPoints() {
        return List.of(new ExampleHistorianExtensionPoint());
    }
}
