package com.inductiveautomation.ignition.examples.historian;

import com.inductiveautomation.historian.common.model.HistorianProperties;
import com.inductiveautomation.historian.common.model.TimeRange;
import com.inductiveautomation.historian.common.model.data.AnnotationPoint;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.ComplexPointType;
import com.inductiveautomation.historian.common.model.data.DataPointType;
import com.inductiveautomation.historian.common.model.data.MetadataPoint;
import com.inductiveautomation.historian.common.model.data.SourceChangePoint;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.historian.common.model.options.AnnotationQueryKey;
import com.inductiveautomation.historian.common.model.options.AnnotationQueryOptions;
import com.inductiveautomation.historian.common.model.options.RawQueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.AbstractHistorian;
import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.historian.gateway.api.query.AbstractQueryEngine;
import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.historian.gateway.api.query.QueryEngine;
import com.inductiveautomation.historian.gateway.api.query.browsing.BrowsePublisher;
import com.inductiveautomation.historian.gateway.api.query.processor.AnnotationPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.DefaultProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.historian.gateway.api.storage.AbstractStorageEngine;
import com.inductiveautomation.historian.gateway.api.storage.StorageEngine;
import com.inductiveautomation.ignition.common.PathTree;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.historian.HistorianBrowseTraits;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.tags.config.properties.WellKnownTagProps;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public class ExampleHistorianProvider extends AbstractHistorian<ExampleHistorianSettings> {
  private final GatewayContext context;
  private final HistorianSettings settings;
  private final QueryEngine queryEngine;
  private final StorageEngine storageEngine;

  private final PathTree<QualifiedPath, Node> nodes =
      new PathTree<>() {
        @Override
        protected PathTree<QualifiedPath, Node>.TreeNode instantiateNode(
            PathTree<QualifiedPath, Node>.TreeNode parent, QualifiedPath path, int pos) {
          TreeNode ret = new TreeNode(parent);
          QualifiedPath nodePath = path == null ? null : path.subpath(0, pos + 1);
          ret.setLeafValue(new Node(nodePath, maxDatapoints, maxAge));
          return ret;
        }
      };

  private final int maxDatapoints;
  private final long maxAge;


  public ExampleHistorianProvider(
      GatewayContext gatewayContext, String name, ExampleHistorianSettings settings) {
    super(gatewayContext, name);
    this.context = gatewayContext;
    this.settings = settings;
    this.queryEngine = new ExampleQueryEngine();
    this.storageEngine = new ExampleStorageEngine();
    this.maxAge = settings.maxAge();
    this.maxDatapoints = settings.maxDatapoints();
  }

  @Override
  public ProfileStatus getStatus() {
    return ProfileStatus.RUNNING;
  }

  @Override
  public Optional<QueryEngine> getQueryEngine() {
    return Optional.of(queryEngine);
  }

  @Override
  public Optional<StorageEngine> getStorageEngine() {
    return Optional.of(storageEngine);
  }

  @Override
  public boolean handleNameChange(String newName) {
    return false;
  }

  @Override
  public boolean handleSettingsChange(ExampleHistorianSettings newSettings) {
    return false;
  }

  @Override
  public ExampleHistorianSettings getSettings() {
    return (ExampleHistorianSettings) this.settings;
  }

  protected Node getOrCreateNode(QualifiedPath path) {
    Node node;
    QualifiedPath normalized = transformPath(path);
    synchronized (nodes) {
      node = nodes.get(normalized);
      if (node == null) {
        node = new Node(normalized, maxDatapoints, maxAge);
        nodes.put(normalized, node);
      }
    }
    return node;
  }

  private class ExampleQueryEngine extends AbstractQueryEngine {
    public ExampleQueryEngine() {
      super(
          context,
          ExampleHistorianProvider.this.historianName,
          ExampleHistorianProvider.this.logger.createSubLogger(QueryEngine.class),
          ExampleHistorianProvider.this.getPathAdapter());
    }

    @Override
    protected Optional<Integer> doQueryAnnotations(AnnotationQueryOptions options,
        AnnotationPointProcessor processor) {
      var validNodes = new HashMap<AnnotationQueryKey, Node>();
      var contextBuilder = DefaultProcessingContext.<AnnotationQueryKey, ComplexPointType>builder();
      AtomicInteger count = new AtomicInteger();

      for (var queryKey : options.getQueryKeys()) {
        var node = nodes.get(pathAdapter.normalize(queryKey.identifier()));
        if (node == null) {
          contextBuilder.addPropertyValue(
              queryKey, HistorianProperties.QUALITY_CODE, QualityCode.Bad_NotFound);
          continue;
        }
        validNodes.put(queryKey, node);
      }
      processor.onInitialize(contextBuilder.build());
      //loop over validNodes and query annotations
      if  (validNodes.isEmpty()) {
        return Optional.empty();
      }
      if (options.getTimeRange().isEmpty()) {
        validNodes.forEach((annotationQueryKey,node) -> {
          processor.onPointAvailable(annotationQueryKey, node.getLatestAnnotation());
          count.incrementAndGet();
        });
      } else {
        validNodes.entrySet().stream()
            .flatMap(
                entry ->
                    entry
                        .getValue()
                        .queryAnnotations(options)
                        .map(val -> new AnnotationQueryResult(entry.getKey(), val)))
            .sorted(Comparator.comparing(a -> a.value.timestamp()))
            .forEach(
                qr -> {
                  processor.onPointAvailable(qr.key, qr.value);
                  count.incrementAndGet();
                });

      }
      processor.onComplete();
      return Optional.of(count.intValue());
    }

    @Override
    protected Optional<? extends HistoricalNode> lookupNode(QualifiedPath qualifiedPath) {
      return Optional.empty();
    }

    @Override
    protected Map<QualifiedPath, ? extends HistoricalNode> queryForHistoricalNodes(
        Set<QualifiedPath> set, @Nullable TimeRange timeRange) {
      return Map.of();
    }

    @Override
    protected Optional<Integer> doQueryRaw(
        RawQueryOptions options, RawPointProcessor processor) {
      var nodeMap = new HashMap<RawQueryKey, Node>();
      var keys = options.getQueryKeys();

      var contextBuilder = DefaultProcessingContext.<RawQueryKey, DataPointType>builder();

      synchronized (nodes) {
        for (RawQueryKey key : keys) {
          Node node = nodes.get(key.source());
          if (node != null) {
            nodeMap.put(key, node);
            contextBuilder.addPropertyValue(key, HistorianProperties.DATA_TYPE, node.getDataType());
          } else {
            logger.tracef("Node not found for %s", key.source());
            contextBuilder.addPropertyValue(
                key, HistorianProperties.QUALITY_CODE, QualityCode.Bad_NotFound);
          }
        }
      }

      processor.onInitialize(contextBuilder.build());

      // How do we do not_found for the bad nodes?

      final var count = new AtomicInteger();
      nodeMap.entrySet().stream()
          .flatMap(e -> e.getValue().query(options).map(v -> new RawQueryResult(e.getKey(), v)))
          .sorted(Comparator.comparing(a -> a.rawPoint.timestamp()))
          .forEach(
              qr -> {
                logger.tracef("Handling value for %s: %s", qr.queryKey, qr.rawPoint);
                processor.onPointAvailable(qr.queryKey, qr.rawPoint);
                count.incrementAndGet();
              });

      processor.onComplete();

      return Optional.of(count.get());
    }


    @Override
    protected boolean isEngineUnavailable() {
      return false;
    }

    @Override
    protected void doBrowse(
        QualifiedPath root, BrowseFilter browseFilter, BrowsePublisher results) {
      logger.debugf("Browsing %s", root);
      List<Node> branchNodes;
      synchronized (nodes) {
        branchNodes = nodes.browse(root);
      }
      for (Node node : branchNodes) {
        // We could do these before the browse, but this handles the case that these events happen
        // while we're adding data.
        if (results.isCanceled()) {
          break; // Stop if we're canceled
        }
        if (browseFilter.hasMaxResults()
            && results.getResultCount() > browseFilter.getMaxResults()) {
          break;
        }

        if (browseFilter.getOrDefaultTrait(HistorianBrowseTraits.INCLUDE_RETIRED)
            || !node.isRetired()) {
          BrowsePublisher.NodeBuilder b =
              results
                  .newNode(node.getTypeId(), node.getName())
                  .creationTime(node.getCreationTime())
                  .retiredTime(node.getRetiredTime());

          if (browseFilter.getOrDefaultTrait(HistorianBrowseTraits.INCLUDE_METADATA)) {
            b = b.metadata(node.getLatestMetadata());
          }

          b = b.hasChildren(!nodes.getChildren(node.getPath()).isEmpty());

          b.add();

          if (browseFilter.isRecursive()) {
            browse(node.getPath(), browseFilter, results.recurse());
          }
        }
      }
    }
  }

  private class ExampleStorageEngine extends AbstractStorageEngine {
    public ExampleStorageEngine() {
      super(
          context,
          ExampleHistorianProvider.this.historianName,
          ExampleHistorianProvider.this.logger.createSubLogger(StorageEngine.class),
          ExampleHistorianProvider.this.getPathAdapter()
      );
    }

    @Override
    protected StorageResult<SourceChangePoint> applySourceChanges(
        List<SourceChangePoint> sourceChangePoints) {
      var succeeded = new LinkedList<SourceChangePoint>();
      var failed = new LinkedList<SourceChangePoint>();
      synchronized (nodes) {
        for (SourceChangePoint sourceChange : sourceChangePoints) {
          QualifiedPath nPrev = sourceChange.oldSource();
          QualifiedPath nCur = sourceChange.newSource();

          Node n = nodes.get(nPrev);
          if (n != null) {
            if (nCur == null) {
              n.retire(sourceChange.timestamp());
              logger.tracef("Retiring %s", nPrev);
            } else {
              n.setPath(nCur);
              nodes.put(nCur, n);
              nodes.remove(nPrev);
              logger.tracef("Renaming %s to %s", nPrev, nCur);
            }
            succeeded.add(sourceChange);
          } else {
            failed.add(sourceChange);
          }
        }
      }
      return StorageResult.of(succeeded, failed);
    }

    @Override
    protected StorageResult<AnnotationPoint> doStoreAnnotations(
        List<AnnotationPoint> annotationPoints) {
      for (AnnotationPoint annotationPoint : annotationPoints) {
        Node node = getOrCreateNode(annotationPoint.source());
        node.addAnnotation(annotationPoint);
        logger.tracef(
            "Storing annotation '%s' for '%s'", annotationPoint, annotationPoint.source());
      }
      return StorageResult.success(annotationPoints);
    }

    @Override
    protected StorageResult<AtomicPoint<?>> doStoreAtomic(List<AtomicPoint<?>> atomicPoints) {
      for (AtomicPoint<?> dataPoint : atomicPoints) {
        Node node = getOrCreateNode(dataPoint.source());
        node.addValue(dataPoint);
        logger.tracef("Storing atomic point '%s' for '%s'", dataPoint, dataPoint.source());
      }
      return StorageResult.success(atomicPoints);
    }

    @Override
    protected StorageResult<MetadataPoint> doStoreMetadata(List<MetadataPoint> metadataPoints) {
      for (MetadataPoint metadata : metadataPoints) {
        Node node = getOrCreateNode(metadata.source());
        node.addMetadata(metadata);
        logger.tracef("Storing metadata '%s' for '%s'", metadata, metadata.source());
      }
      return StorageResult.success(metadataPoints);
    }

    @Override
    protected boolean isEngineUnavailable() {
      return false;
    }
  }

  protected abstract static class TimeSortedList<T> extends LinkedList<T> {
    private final int maxSize;
    private final long maxAge;

    protected TimeSortedList() {
      this(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    protected TimeSortedList(int maxSize, long maxAge) {
      this.maxSize = maxSize;
      this.maxAge = maxAge;
    }

    protected abstract Instant getTimestamp(T value);

    public int indexOf(Instant key) {
      // Run a binary search on the list to find the insertion point based on getTimestamp for the
      // value
      int low = 0;
      int high = size() - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        Instant midVal = getTimestamp(get(mid));
        int cmp = midVal.compareTo(key);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          low = mid;
          break;
        }
      }
      return low;
    }

    @Override
    public boolean add(T t) {
      while (size() > maxSize) {
        removeFirst();
      }
      while (!isEmpty()
          && getTimestamp(getFirst()).toEpochMilli() < System.currentTimeMillis() - maxAge) {
        removeFirst();
      }
      add(indexOf(getTimestamp(t)), t);
      return true;
    }
  }

  protected static class Node {
    protected QualifiedPath path;
    protected Instant creationTime = Instant.now();
    protected Instant retiredTime;

    protected final TimeSortedList<AtomicPoint<?>> rawPoints;
    protected final TimeSortedList<AnnotationPoint> annotations;
    protected final TimeSortedList<MetadataPoint> metadata;

    protected Node(QualifiedPath path, int maxSize, long maxAge) {
      this.path = path;
      rawPoints =
          new TimeSortedList<>(maxSize, maxAge) {
            @Override
            protected Instant getTimestamp(AtomicPoint<?> value) {
              return value.timestamp();
            }
          };
      annotations =
          new TimeSortedList<>(maxSize, maxAge) {
            @Override
            protected Instant getTimestamp(AnnotationPoint value) {
              return value.timestamp();
            }
          };
      metadata =
          new TimeSortedList<>(maxSize, maxAge) {
            @Override
            protected Instant getTimestamp(MetadataPoint value) {
              return value.timestamp();
            }
          };
    }

    public QualifiedPath getPath() {
      return path;
    }

    public String getTypeId() {
      return path.getLastPathComponentId();
    }

    public String getName() {
      return path.getLastPathComponent();
    }

    public void setPath(QualifiedPath path) {
      this.path = path;
    }

    public Instant getCreationTime() {
      return creationTime;
    }

    // isRetired
    public boolean isRetired() {
      return retiredTime != null;
    }

    // getRetiredTime
    public Instant getRetiredTime() {
      return retiredTime;
    }

    // retire
    public void retire() {
      retire(Instant.now());
    }

    public void retire(Instant time) {
      retiredTime = time;
    }

    // addValue
    public void addValue(AtomicPoint<?> point) {
      rawPoints.add(point);
    }

    // addAnnotation
    public void addAnnotation(AnnotationPoint annotation) {
      annotations.add(annotation);
    }

    // addmetadata
    public void addMetadata(MetadataPoint metadata) {
      this.metadata.add(metadata);
    }

    // getLatestMetadata
    public MetadataPoint getLatestMetadata() {
      return metadata.isEmpty() ? null : metadata.getLast();
    }

    //getLatestAnnotation
    public AnnotationPoint getLatestAnnotation() {
      return annotations.isEmpty() ? null : annotations.getLast();
    }

    public Stream<AnnotationPoint> queryAnnotations(AnnotationQueryOptions options) {
      synchronized (annotations) {
        var timeRange = options.getTimeRange();
        if (timeRange.isEmpty()) {
          return Stream.of(annotations.get(annotations.size() - 1));
        }

        var startDate = timeRange.get().startTime();
        var endDate = timeRange.get().endTime();
        int start = annotations.indexOf(startDate);
        int end = annotations.indexOf(endDate);

        if (timeRange.get().includeBounds()) {
          if (start > 0) {
            start--;
          }
          if (end < annotations.size() - 1) {
            end++;
          }
        }

        return annotations.subList(start, end).stream();
      }
    }

    public DataType getDataType() {
      MetadataPoint m = getLatestMetadata();
      return m == null ? DataType.Int4 : m.value().getOrDefault(WellKnownTagProps.DataType);
    }

    public Stream<AtomicPoint<?>> query(RawQueryOptions options) {
      synchronized (rawPoints) {
        var timeRange = options.getTimeRange();
        if (timeRange.isEmpty()) {
          return Stream.of(rawPoints.get(rawPoints.size() - 1));
        }

        var startDate = timeRange.get().startTime();
        var endDate = timeRange.get().endTime();
        int start = rawPoints.indexOf(startDate);
        int end = rawPoints.indexOf(endDate);

        if (timeRange.get().includeBounds()) {
          if (start > 0) {
            start--;
          }
          if (end < rawPoints.size() - 1) {
            end++;
          }
        }

        return rawPoints.subList(start, end).stream();
      }
    }
  }

  protected record RawQueryResult(RawQueryKey queryKey, AtomicPoint<?> rawPoint) {}
  protected record AnnotationQueryResult(AnnotationQueryKey key, AnnotationPoint value) {}

}
