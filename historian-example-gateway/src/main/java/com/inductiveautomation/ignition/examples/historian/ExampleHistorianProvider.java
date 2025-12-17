package com.inductiveautomation.ignition.examples.historian;

import com.codahale.metrics.Histogram;
import com.inductiveautomation.historian.common.model.AggregationType;
import com.inductiveautomation.historian.common.model.TimeRange;
import com.inductiveautomation.historian.common.model.data.*;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.historian.common.model.options.ComplexQueryKey;
import com.inductiveautomation.historian.common.model.options.ComplexQueryOptions;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.AbstractHistorian;
import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.historian.gateway.api.query.AbstractQueryEngine;
import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.historian.gateway.api.query.HistoricalNodeTree;
import com.inductiveautomation.historian.gateway.api.query.QueryEngine;
import com.inductiveautomation.historian.gateway.api.query.browsing.BrowsePublisher;
import com.inductiveautomation.historian.gateway.api.query.processor.AggregatedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.ComplexPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.historian.gateway.api.storage.AbstractStorageEngine;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.QualifiedPathUtils;
import com.inductiveautomation.ignition.common.TypeUtilities;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.util.LoggerEx;
import com.inductiveautomation.ignition.examples.historian.model.DuckDBHistoricalNode;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.python.antlr.PythonParser.continue_stmt_return;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

import static java.util.Objects.requireNonNullElse;
import static java.util.Objects.requireNonNullElseGet;

public class ExampleHistorianProvider extends AbstractHistorian<ExampleHistorianSettings> {
    private static final LoggerEx LOGGER = LoggerEx.newBuilder().build(ExampleHistorianProvider.class);
    private final GatewayContext context;
    private final HistorianSettings settings;
    private final QueryEngine queryEngine;
    private final StorageEngine storageEngine;
    private String path;

    DuckDBConnection connection;

    private boolean tablesVerified = false;

    private final Map<QualifiedPath, DuckDBHistoricalNode> nodeCache = new HashMap<>();
    private final NodeTree nodeTree = new NodeTree();

    public ExampleHistorianProvider(GatewayContext gatewayContext, String name, ExampleHistorianSettings settings) {
        super(gatewayContext, name);
        this.context = gatewayContext;
        this.settings = settings;

        try {
            this.path = gatewayContext.getSystemManager().getDataDir().toString() + "/var/duckdb/" + name;
            Files.createDirectories(Path.of(path).getParent());

            connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + path);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.queryEngine = new ExampleQueryEngine();
        this.storageEngine = new StorageEngine();

    }

    @Override
    protected void onStartup() throws Exception {
        logger.info("Starting DuckDB Historian " + historianName);
        createTables(connection);
        loadCache(connection);
    }

    private void loadCache(DuckDBConnection connection) throws SQLException, IOException {
        logger.info("Start loading cache for historian provider \"" + historianName + "\"");
        var QUERY = "SELECT * FROM definitions";
        Statement stmt = connection.createStatement();

        try (ResultSet rs = stmt.executeQuery(QUERY)) {
            while (rs.next()) {
                UUID node_id = UUID.fromString(rs.getString("node_id"));
                String source = rs.getString("source");
                logger.info(node_id.toString() + ", " + source);
                nodeCache.put(QualifiedPath.parse(source),
                        new DuckDBHistoricalNode(node_id, QualifiedPath.parse(source),
                                Optional.of(DataType.Float8), Optional.of(DuckDBColumnType.DOUBLE)));
            }
        }

        logger.info("Finished loading cache for historian provider \"" + historianName + "\"");
    }

    @Override
    public ProfileStatus getStatus() {
        return tablesVerified ? ProfileStatus.RUNNING : ProfileStatus.ERRORED;
    }

    @Override
    public Optional<QueryEngine> getQueryEngine() {
        return super.getQueryEngine();
    }

    @Override
    public Optional<com.inductiveautomation.historian.gateway.api.storage.StorageEngine> getStorageEngine() {
        return Optional.ofNullable(this.storageEngine);
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

    class ExampleQueryEngine extends AbstractQueryEngine {

        protected ExampleQueryEngine() {
            super(context, ExampleHistorianProvider.this.historianName, LOGGER);
        }

        @Override
        protected void doBrowse(QualifiedPath root, BrowseFilter filter, BrowsePublisher results) {
            // TODO Auto-generated method stub
            results.publishNode(root, filter, results);
            throw new UnsupportedOperationException("Unimplemented method 'doBrowse'");
        }

        @Override
        protected Optional<Integer> doQueryRaw(RawQueryOptions arg0, RawPointProcessor arg1) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'doQueryRaw'");
        }

        @Override
        protected boolean isEngineUnavailable() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'isEngineUnavailable'");
        }

        @Override
        protected Optional<? extends HistoricalNode> lookupNode(QualifiedPath arg0) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'lookupNode'");
        }

        @Override
        protected Map<QualifiedPath, ? extends HistoricalNode> queryForHistoricalNodes(Set<QualifiedPath> arg0,
                @Nullable TimeRange arg1) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'queryForHistoricalNodes'");
        }

    }

    class StorageEngine extends AbstractStorageEngine {

        private Instant last_checkpoint = Instant.now();

        protected StorageEngine() {
            super(context, ExampleHistorianProvider.this.historianName,
                    ExampleHistorianProvider.this.logger.createSubLogger(StorageEngine.class),
                    ExampleHistorianProvider.this.getPathAdapter());
        }

        @Override
        protected StorageResult<SourceChangePoint> applySourceChanges(List<SourceChangePoint> list) {
            logger.info("Applying source changes");
            return StorageResult.of(list, List.of());
        }

        @Override
        protected StorageResult<AtomicPoint<?>> doStoreAtomic(List<AtomicPoint<?>> list) {
            var successPoints = new ArrayList<AtomicPoint<?>>();
            var failedPoints = new ArrayList<AtomicPoint<?>>();

            try (

                    var def_appender = connection.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "definitions");
                    var datapoints_appender = connection.createAppender(DuckDBConnection.DEFAULT_SCHEMA,
                            "datapoints")) {
                for (var point : list) {
                    DuckDBHistoricalNode ddbNode;
                    if (!nodeCache.containsKey(point.source())) {
                        logger.info("Adding new node");
                        ddbNode = DuckDBHistoricalNode.newBuilder().source(point.source()).dataType(DataType.Float8)
                                .columnType(DuckDBColumnType.DOUBLE).build();
                        nodeCache.put(point.source(), ddbNode);
                        def_appender.beginRow();
                        def_appender.append(ddbNode.nodeId());
                        def_appender.append(ddbNode.source().toString());
                        def_appender.append(DataType.getTypeForClass(point.value().getClass()).getIntValue());
                        def_appender.append(LocalDateTime.now());
                        def_appender.appendNull();
                        def_appender.endRow();
                        logger.info("Done adding node");
                    } else {
                        ddbNode = nodeCache.get(point.source());
                    }
                    datapoints_appender.beginRow();
                    datapoints_appender.append(ddbNode.nodeId());
                    datapoints_appender
                            .append(LocalDateTime.ofInstant(point.timestamp(), ZoneId.of("America/Edmonton")));
                    datapoints_appender.append(TypeUtilities.toDouble(point.value()));
                    datapoints_appender.appendNull();
                    datapoints_appender.appendNull();
                    datapoints_appender.appendNull();
                    datapoints_appender.appendNull();
                    datapoints_appender.append(point.quality().getCode());
                    datapoints_appender.append(point.quality().getLevel().bits());
                    datapoints_appender
                            .append(LocalDateTime.ofInstant(point.timestamp(), ZoneId.of("America/Edmonton")));
                    datapoints_appender.endRow();
                    successPoints.add(point);
                }
                if (Instant.now().isAfter(last_checkpoint.plusSeconds(10))) {
                    last_checkpoint = Instant.now();
                    try (var stmt = connection.createStatement()) {
                        stmt.execute("CHECKPOINT");
                    }
                }
            } catch (SQLException e) {
                logger.error("Error", e);
                throw new RuntimeException(e);
            } catch (Exception e) {
                logger.error("Error", e);
                throw new RuntimeException(e);
            }
            return StorageResult.of(successPoints, failedPoints);
        }

        @Override
        protected boolean isEngineUnavailable() {
            return !tablesVerified;
        }

    }

    private void createTables(DuckDBConnection conn) throws SQLException {
        var definitions_table = "CREATE TABLE IF NOT EXISTS definitions (node_id UUID, source VARCHAR, data_type int4, created_at TIMESTAMP, retired_at TIMESTAMP)";
        var annotations_table = "CREATE TABLE IF NOT EXISTS annotations (annotation_id UUID, node_id UUID, type VARCHAR, start_at TIMESTAMP, end_at TIMESTAMP, notes VARCHAR, is_deleted boolean, changed_at TIMESTAMP)";
        var metadata_table = "CREATE TABLE IF NOT EXISTS metadata (node_id UUID, metadata JSON, metadata_tstamp TIMESTAMP)";
        var datapoints_table = "CREATE TABLE IF NOT EXISTS datapoints (node_id UUID, value_time TIMESTAMP, double_value DOUBLE, long_value LONG, string_value varchar, ts_value TIMESTAMP, bool_value BOOLEAN, quality_code INT4, quality_level INT4, snapshot_time TIMESTAMP)";

        var tables = new String[] { definitions_table, annotations_table, metadata_table, datapoints_table };

        for (var table : tables) {
            try (var stmt = conn.createStatement()) {
                stmt.execute(table);
            }
        }

        tablesVerified = true;
    }

    public static class NodeTree extends HistoricalNodeTree<DuckDBHistoricalNode>{

        private NodeTree(Function<QualifiedPath, DuckDBHistoricalNode> nodeFactory) {
            super(DuckDBHistoricalNode::createImplicitNode);
        }
        
        private void registerNode(DuckDBHistoricalNode node) {
         withWriteLock(() -> {
            put(node.source(), node);
            updateParentRetirementState(node);
         });   
        }
        
        private void recursiveRetireWithParent(DuckDBHistoricalNode node) {
            withWriteLock(() -> {
                var path = node.source();
                if (node.retiredTime().isEmpty()) {
                    return;
                }

                var treeNode = super.findNode(path, false);
                if (treeNode == null) {
                    return;
                }
                
                treeNode.setLeafValue(node);

                var parentPath = path.getParentPath();
                if (parentPath.getParentLength() == 0) {
                    return;
                }

                recursiveRetireWithParent(parentPath);
            });    
        }

        private void recursiveRetireWithParent(QualifiedPath path) {
            withWriteLock(() -> {
                var treeNode = super.findNode(path, false);
                if (treeNode == null) {
                    return;
                }

                if (areAnyNodesUnretired(treeNode.getChildrenValues())){
                    return;
                }
                
                var parentPath = path.getParentPath();
                if (parentPath.getParentLength() == 0) {
                    return;
                }

                var parentNode = super.findNode(parentPath, false);
                if (parentNode == null && areAllNodesRetired(treeNode.getChildrenValues())) {
                    recursiveRetireWithParent(parentPath);
                }
            });    
        }
        
        private void updateParentRetirementState(DuckDBHistoricalNode node) {
            withWriteLock(() -> {
                node.retiredTime().ifPresentOrElse(
                    rt -> recursiveRetireWithParent(node),
                    () -> recursiveUnretireWithParent(node.source())
                );
            });
        }
    }
}
