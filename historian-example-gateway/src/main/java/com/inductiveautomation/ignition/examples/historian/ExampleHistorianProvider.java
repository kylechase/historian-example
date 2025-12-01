package com.inductiveautomation.ignition.examples.historian;

import com.inductiveautomation.historian.common.model.AggregationType;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.ChangePoint;
import com.inductiveautomation.historian.common.model.data.ComplexPoint;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.historian.common.model.options.ComplexQueryKey;
import com.inductiveautomation.historian.common.model.options.ComplexQueryOptions;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.AbstractHistorian;
import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.historian.gateway.api.query.QueryEngine;
import com.inductiveautomation.historian.gateway.api.query.browsing.BrowsePublisher;
import com.inductiveautomation.historian.gateway.api.query.processor.AggregatedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.ComplexPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.historian.gateway.api.storage.StorageEngine;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class ExampleHistorianProvider extends AbstractHistorian<ExampleHistorianSettings> {
    private final GatewayContext context;
    private final HistorianSettings settings;

    ExampleHistorianProvider(GatewayContext gatewayContext, String name, ExampleHistorianSettings settings) {
        super(gatewayContext, name);
        this.context = gatewayContext;
        this.settings = settings;
    }

    @Override
    public ProfileStatus getStatus() {
        return ProfileStatus.RUNNING;
    }

    @Override
    public Optional<QueryEngine> getQueryEngine() {
        return super.getQueryEngine();
    }

    @Override
    public Optional<StorageEngine> getStorageEngine() {
        return super.getStorageEngine();
    }

    @Override
    public boolean handleNameChange(String newName) {
        return super.handleNameChange(newName);
    }

    @Override
    public boolean handleSettingsChange(ExampleHistorianSettings newSettings) {
        return super.handleSettingsChange(newSettings);
    }

    @Override
    public ExampleHistorianSettings getSettings() {
        return super.getSettings();
    }

    class ExampleQueryEngine implements QueryEngine {

        @Override
        public void browse(QualifiedPath qualifiedPath, BrowseFilter browseFilter, BrowsePublisher browsePublisher) {

        }

        @Override
        public void query(RawQueryOptions rawQueryOptions, RawPointProcessor rawPointProcessor) {

        }

        @Override
        public void query(AggregatedQueryOptions options, AggregatedPointProcessor processor) {
            QueryEngine.super.query(options, processor);
        }

        @Override
        public <P extends ComplexPoint<?>, K extends ComplexQueryKey<?>> void query(ComplexQueryOptions<K> options, ComplexPointProcessor<P, K> processor) {
            QueryEngine.super.query(options, processor);
        }

        @Override
        public Collection<? extends AggregationType> getNativeAggregates() {
            return QueryEngine.super.getNativeAggregates();
        }
    }

    class ExampleStorageEngine implements StorageEngine {

        @Override
        public CompletionStage<StorageResult<AtomicPoint<?>>> storeAtomic(List<AtomicPoint<?>> list) {
            return null;
        }

        @Override
        public <C extends ComplexPoint<?>> CompletionStage<StorageResult<C>> storeComplex(List<C> list) {
            return null;
        }

        @Override
        public <C extends ChangePoint<?>> CompletionStage<StorageResult<C>> applyChanges(List<C> list) {
            return null;
        }
    }
}
