/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.loader.CSVLoader;
import org.apache.solr.handler.loader.JavabinLoader;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.reporters.solr.SolrReporter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to collect and aggregate metric reports.  Each report indicates the target registry where
 * metrics values should be collected and aggregated. Metrics with the same names are
 * aggregated using {@link AggregateMetric} instances, which track the source of updates and
 * their count, as well as providing simple statistics over collected values.
 *
 * Each report consists of {@link SolrInputDocument}-s that are expected to contain
 * the following fields:
 * <ul>
 *   <li>{@link SolrReporter#GROUP_ID} - (required) specifies target registry name where metrics will be grouped.</li>
 *   <li>{@link SolrReporter#REPORTER_ID} - (required) id of the reporter that sent this update. This can be eg.
 *   node name or replica name or other id that uniquely identifies the source of metrics values.</li>
 *   <li>{@link MetricUtils#METRIC_NAME} - (required) metric name (in the source registry)</li>
 *   <li>{@link SolrReporter#LABEL_ID} - (optional) label to prepend to metric names in the target registry.</li>
 *   <li>{@link SolrReporter#REGISTRY_ID} - (optional) name of the source registry.</li>
 * </ul>
 * Remaining fields are assumed to be single-valued, and to contain metric attributes and their values. Example:
 * <pre>
 *   &lt;doc&gt;
 *     &lt;field name="_group_"&gt;solr.core.collection1.shard1.leader&lt;/field&gt;
 *     &lt;field name="_reporter_"&gt;core_node3&lt;/field&gt;
 *     &lt;field name="metric"&gt;INDEX.merge.errors&lt;/field&gt;
 *     &lt;field name="value"&gt;0&lt;/field&gt;
 *   &lt;/doc&gt;
 * </pre>
 *
 * @deprecated this class (and the related functionality of {@link MetricsHistoryHandler} willl be removed in Solr 9.0.
 */
public class MetricsCollectorHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String HANDLER_PATH = "/admin/metrics/collector";

  private final CoreContainer coreContainer;
  private final SolrMetricManager metricManager;
  private final Map<String, ContentStreamLoader> loaders = new HashMap<>();
  private SolrParams params;

  public MetricsCollectorHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.metricManager = coreContainer.getMetricManager();

  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList initArgs) {
    super.init(initArgs);
    if (initArgs != null) {
      params = initArgs.toSolrParams();
    } else {
      params = new ModifiableSolrParams();
    }
    loaders.put("application/xml", new XMLLoader().init(params) );
    loaders.put("application/json", new JsonLoader().init(params) );
    loaders.put("application/csv", new CSVLoader().init(params) );
    loaders.put("application/javabin", new JavabinLoader().init(params) );
    loaders.put("text/csv", loaders.get("application/csv") );
    loaders.put("text/xml", loaders.get("application/xml") );
    loaders.put("text/json", loaders.get("application/json"));
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (coreContainer == null || coreContainer.isShutDown()) {
      // silently drop request
      return;
    }
    //log.info("#### {}", req);
    if (req.getContentStreams() == null) { // no content
      return;
    }
    for (ContentStream cs : req.getContentStreams()) {
      if (cs.getContentType() == null) {
        log.warn("Missing content type - ignoring");
        continue;
      }
      ContentStreamLoader loader = loaders.get(cs.getContentType());
      if (loader == null) {
        throw new SolrException(SolrException.ErrorCode.UNSUPPORTED_MEDIA_TYPE, "Unsupported content type for stream: " + cs.getSourceInfo() + ", contentType=" + cs.getContentType());
      }
      loader.load(req, rsp, cs, new MetricUpdateProcessor(metricManager));
    }
  }

  @Override
  public String getDescription() {
    return "Handler for collecting and aggregating SolrCloud metric reports.";
  }

  private static class MetricUpdateProcessor extends UpdateRequestProcessor {
    private final SolrMetricManager metricManager;

    public MetricUpdateProcessor(SolrMetricManager metricManager) {
      super(null);
      this.metricManager = metricManager;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.solrDoc;
      if (doc == null) {
        return;
      }
      String metricName = (String)doc.getFieldValue(MetricUtils.METRIC_NAME);
      if (metricName == null) {
        log.warn("Missing {} field in document, skipping: {}", MetricUtils.METRIC_NAME, doc);
        return;
      }
      doc.remove(MetricUtils.METRIC_NAME);
      // XXX we could modify keys by using this original registry name
      doc.remove(SolrReporter.REGISTRY_ID);
      String groupId = (String)doc.getFieldValue(SolrReporter.GROUP_ID);
      if (groupId == null) {
        log.warn("Missing {}  field in document, skipping: {}", SolrReporter.GROUP_ID, doc);
        return;
      }
      doc.remove(SolrReporter.GROUP_ID);
      String reporterId = (String)doc.getFieldValue(SolrReporter.REPORTER_ID);
      if (reporterId == null) {
        log.warn("Missing {} field in document, skipping: {}", SolrReporter.REPORTER_ID, doc);
        return;
      }
      doc.remove(SolrReporter.REPORTER_ID);
      String labelId = (String)doc.getFieldValue(SolrReporter.LABEL_ID);
      doc.remove(SolrReporter.LABEL_ID);
      doc.forEach(f -> {
        String key;
        if (doc.size() == 1 && f.getName().equals(MetricUtils.VALUE)) {
          // only one "value" field - skip the unnecessary field name
          key = MetricRegistry.name(labelId, metricName);
        } else {
          key = MetricRegistry.name(labelId, metricName, f.getName());
        }
        MetricRegistry registry = metricManager.registry(groupId);
        AggregateMetric metric = getOrCreate(registry, key);
        Object o = f.getFirstValue();
        if (o != null) {
          metric.set(reporterId, o);
        } else {
          // remove missing values
          metric.clear(reporterId);
        }
      });
    }

    private AggregateMetric getOrCreate(MetricRegistry registry, String name) {
      AggregateMetric existing = (AggregateMetric)registry.getMetrics().get(name);
      if (existing != null) {
        return existing;
      }
      AggregateMetric add = new AggregateMetric();
      try {
        registry.register(name, add);
        return add;
      } catch (IllegalArgumentException e) {
        // someone added before us
        existing = (AggregateMetric)registry.getMetrics().get(name);
        if (existing == null) { // now, that is weird...
          throw new IllegalArgumentException("Inconsistent metric status, " + name);
        }
        return existing;
      }
    }

    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processDelete");
    }

    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processMergeIndexes");
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processCommit");
    }

    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processRollback");
    }
  }
}
