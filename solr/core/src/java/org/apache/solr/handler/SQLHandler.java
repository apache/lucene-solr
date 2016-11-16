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
package org.apache.solr.handler;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

import com.facebook.presto.sql.tree.*;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.FacetStream;
import org.apache.solr.client.solrj.io.stream.ParallelStream;
import org.apache.solr.client.solrj.io.stream.RankStream;
import org.apache.solr.client.solrj.io.stream.RollupStream;
import org.apache.solr.client.solrj.io.stream.SelectStream;
import org.apache.solr.client.solrj.io.stream.StatsStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.UniqueStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.parser.SqlParser;

public class SQLHandler extends RequestHandlerBase implements SolrCoreAware , PermissionNameProvider {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static String defaultZkhost = null;
  private static String defaultWorkerCollection = null;

  static final String sqlNonCloudErrorMsg = "/sql handler only works in Solr Cloud mode";

  private boolean isCloud = false;

  public void inform(SolrCore core) {
    CoreContainer coreContainer = core.getCoreDescriptor().getCoreContainer();

    if(coreContainer.isZooKeeperAware()) {
      defaultZkhost = core.getCoreDescriptor().getCoreContainer().getZkController().getZkServerAddress();
      defaultWorkerCollection = core.getCoreDescriptor().getCollectionName();
      isCloud = true;
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    params = adjustParams(params);
    req.setParams(params);
    String sql = params.get("stmt");
    int numWorkers = params.getInt("numWorkers", 1);
    String workerCollection = params.get("workerCollection", defaultWorkerCollection);
    String workerZkhost = params.get("workerZkhost", defaultZkhost);
    String mode = params.get("aggregationMode", "map_reduce");
    StreamContext context = new StreamContext();

    // JDBC driver requires metadata from the SQLHandler. Default to false since this adds a new Metadata stream.
    boolean includeMetadata = params.getBool("includeMetadata", false);

    try {

      if(!isCloud) {
        throw new IllegalStateException(sqlNonCloudErrorMsg);
      }

      if(sql == null) {
        throw new Exception("stmt parameter cannot be null");
      }

      context.setSolrClientCache(StreamHandler.clientCache);

      TupleStream tupleStream = SQLTupleStreamParser.parse(sql,
                                                           numWorkers,
                                                           workerCollection,
                                                           workerZkhost,
                                                           AggregationMode.getMode(mode),
                                                           includeMetadata,
                                                           context);

      rsp.add("result-set", new StreamHandler.TimerStream(new ExceptionStream(tupleStream)));
    } catch(Exception e) {
      //Catch the SQL parsing and query transformation exceptions.
      SolrException.log(logger, e);
      rsp.add("result-set", new StreamHandler.DummyErrorStream(e));
    }
  }

  private SolrParams adjustParams(SolrParams params) {
    ModifiableSolrParams adjustedParams = new ModifiableSolrParams(params);
    adjustedParams.set(CommonParams.OMIT_HEADER, "true");
    return adjustedParams;
  }

  public String getDescription() {
    return "SQLHandler";
  }

  public String getSource() {
    return null;
  }

  public static class SQLTupleStreamParser {

    public static TupleStream parse(String sql,
                                    int numWorkers,
                                    String workerCollection,
                                    String workerZkhost,
                                    AggregationMode aggregationMode,
                                    boolean includeMetadata,
                                    StreamContext context) throws IOException {
      SqlParser parser = new SqlParser();
      Statement statement = parser.createStatement(sql);

      SQLVisitor sqlVistor = new SQLVisitor(new StringBuilder());

      sqlVistor.process(statement, new Integer(0));
      sqlVistor.reverseAliases();

      TupleStream sqlStream = null;

      if(sqlVistor.table.toUpperCase(Locale.ROOT).contains("_CATALOGS_")) {
        sqlStream = new SelectStream(new CatalogsStream(defaultZkhost), sqlVistor.columnAliases);
      } else if(sqlVistor.table.toUpperCase(Locale.ROOT).contains("_SCHEMAS_")) {
        sqlStream = new SelectStream(new SchemasStream(defaultZkhost), sqlVistor.columnAliases);
      } else if(sqlVistor.table.toUpperCase(Locale.ROOT).contains("_TABLES_")) {
        sqlStream = new SelectStream(new TableStream(defaultZkhost), sqlVistor.columnAliases);
      } else if(sqlVistor.groupByQuery) {
        if(aggregationMode == AggregationMode.FACET) {
          sqlStream = doGroupByWithAggregatesFacets(sqlVistor);
        } else {
          context.numWorkers = numWorkers;
          sqlStream = doGroupByWithAggregates(sqlVistor, numWorkers, workerCollection, workerZkhost);
        }
      } else if(sqlVistor.isDistinct) {
        if(aggregationMode == AggregationMode.FACET) {
          sqlStream = doSelectDistinctFacets(sqlVistor);
        } else {
          context.numWorkers = numWorkers;
          sqlStream = doSelectDistinct(sqlVistor, numWorkers, workerCollection, workerZkhost);
        }
      } else {
        sqlStream = doSelect(sqlVistor);
      }

      if(includeMetadata) {
        sqlStream = new MetadataStream(sqlStream, sqlVistor);
      }

      sqlStream.setStreamContext(context);
      return sqlStream;
    }
  }

  private static TupleStream doGroupByWithAggregates(SQLVisitor sqlVisitor,
                                                     int numWorkers,
                                                     String workerCollection,
                                                     String workerZkHost) throws IOException {

    Set<String> fieldSet = new HashSet();
    Bucket[] buckets = getBuckets(sqlVisitor.groupBy, fieldSet);
    Metric[] metrics = getMetrics(sqlVisitor.fields, fieldSet);
    if(metrics.length == 0) {
      throw new IOException("Group by queries must include atleast one aggregate function.");
    }

    String fl = fields(fieldSet);
    String sortDirection = getSortDirection(sqlVisitor.sorts);
    String sort = bucketSort(buckets, sortDirection);

    TableSpec tableSpec = new TableSpec(sqlVisitor.table, defaultZkhost);

    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.FL, fl);
    params.set(CommonParams.Q, sqlVisitor.query);
    //Always use the /export handler for Group By Queries because it requires exporting full result sets.
    params.set(CommonParams.QT, "/export");

    if(numWorkers > 1) {
      params.set("partitionKeys", getPartitionKeys(buckets));
    }

    params.set("sort", sort);

    TupleStream tupleStream = null;

    CloudSolrStream cstream = new CloudSolrStream(zkHost, collection, params);
    tupleStream = new RollupStream(cstream, buckets, metrics);

    if(numWorkers > 1) {
      // Do the rollups in parallel
      // Maintain the sort of the Tuples coming from the workers.
      StreamComparator comp = bucketSortComp(buckets, sortDirection);
      ParallelStream parallelStream = new ParallelStream(workerZkHost, workerCollection, tupleStream, numWorkers, comp);

      StreamFactory factory = new StreamFactory()
          .withFunctionName("search", CloudSolrStream.class)
          .withFunctionName("parallel", ParallelStream.class)
          .withFunctionName("rollup", RollupStream.class)
          .withFunctionName("sum", SumMetric.class)
          .withFunctionName("min", MinMetric.class)
          .withFunctionName("max", MaxMetric.class)
          .withFunctionName("avg", MeanMetric.class)
          .withFunctionName("count", CountMetric.class);

      parallelStream.setStreamFactory(factory);
      tupleStream = parallelStream;
    }

    //TODO: This should be done on the workers, but it won't serialize because it relies on Presto classes.
    // Once we make this a Expressionable the problem will be solved.

    if(sqlVisitor.havingExpression != null) {
      tupleStream = new HavingStream(tupleStream, sqlVisitor.havingExpression, sqlVisitor.reverseColumnAliases );
    }

    if(sqlVisitor.sorts != null && sqlVisitor.sorts.size() > 0) {
      if(!sortsEqual(buckets, sortDirection, sqlVisitor.sorts, sqlVisitor.reverseColumnAliases)) {
        int limit = sqlVisitor.limit == -1 ? 100 : sqlVisitor.limit;
        StreamComparator comp = getComp(sqlVisitor.sorts, sqlVisitor.reverseColumnAliases);
        //Rank the Tuples
        //If parallel stream is used ALL the Rolled up tuples from the workers will be ranked
        //Providing a true Top or Bottom.
        tupleStream = new RankStream(tupleStream, limit, comp);
      } else {
        // Sort is the same as the same as the underlying stream
        // Only need to limit the result, not Rank the result
        if(sqlVisitor.limit > -1) {
          tupleStream = new LimitStream(tupleStream, sqlVisitor.limit);
        }
      }
    }

    if(sqlVisitor.hasColumnAliases) {
      tupleStream = new SelectStream(tupleStream, sqlVisitor.columnAliases);
    }

    return tupleStream;
  }

  private static TupleStream doSelectDistinct(SQLVisitor sqlVisitor,
                                              int numWorkers,
                                              String workerCollection,
                                              String workerZkHost) throws IOException {

    Set<String> fieldSet = new HashSet();
    Bucket[] buckets = getBuckets(sqlVisitor.fields, fieldSet);
    Metric[] metrics = getMetrics(sqlVisitor.fields, fieldSet);

    if(metrics.length > 0) {
      throw new IOException("Select Distinct queries cannot include aggregate functions.");
    }

    String fl = fields(fieldSet);

    String sort = null;
    StreamEqualitor ecomp = null;
    StreamComparator comp = null;

    if(sqlVisitor.sorts != null && sqlVisitor.sorts.size() > 0) {
      StreamComparator[] adjustedSorts = adjustSorts(sqlVisitor.sorts, buckets, sqlVisitor.reverseColumnAliases);
        // Because of the way adjustSorts works we know that each FieldComparator has a single
        // field name. For this reason we can just look at the leftFieldName
      FieldEqualitor[] fieldEqualitors = new FieldEqualitor[adjustedSorts.length];
      StringBuilder buf = new StringBuilder();
      for(int i=0; i<adjustedSorts.length; i++) {
        FieldComparator fieldComparator = (FieldComparator)adjustedSorts[i];
        fieldEqualitors[i] = new FieldEqualitor(fieldComparator.getLeftFieldName());
        if(i>0) {
          buf.append(",");
        }
        buf.append(fieldComparator.getLeftFieldName()).append(" ").append(fieldComparator.getOrder().toString());
      }

      sort = buf.toString();

      if(adjustedSorts.length == 1) {
        ecomp = fieldEqualitors[0];
        comp = adjustedSorts[0];
      } else {
        ecomp = new MultipleFieldEqualitor(fieldEqualitors);
        comp = new MultipleFieldComparator(adjustedSorts);
      }
    } else {
      StringBuilder sortBuf = new StringBuilder();
      FieldEqualitor[] equalitors = new FieldEqualitor[buckets.length];
      StreamComparator[] streamComparators = new StreamComparator[buckets.length];
      for(int i=0; i<buckets.length; i++) {
        equalitors[i] = new FieldEqualitor(buckets[i].toString());
        streamComparators[i] = new FieldComparator(buckets[i].toString(), ComparatorOrder.ASCENDING);
        if(i>0) {
          sortBuf.append(',');
        }
        sortBuf.append(buckets[i].toString()).append(" asc");
      }

      sort = sortBuf.toString();

      if(equalitors.length == 1) {
        ecomp = equalitors[0];
        comp = streamComparators[0];
      } else {
        ecomp = new MultipleFieldEqualitor(equalitors);
        comp = new MultipleFieldComparator(streamComparators);
      }
    }

    TableSpec tableSpec = new TableSpec(sqlVisitor.table, defaultZkhost);

    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.FL, fl);
    params.set(CommonParams.Q, sqlVisitor.query);
    //Always use the /export handler for Distinct Queries because it requires exporting full result sets.
    params.set(CommonParams.QT, "/export");

    if(numWorkers > 1) {
      params.set("partitionKeys", getPartitionKeys(buckets));
    }

    params.set("sort", sort);

    TupleStream tupleStream = null;

    CloudSolrStream cstream = new CloudSolrStream(zkHost, collection, params);
    tupleStream = new UniqueStream(cstream, ecomp);

    if(numWorkers > 1) {
      // Do the unique in parallel
      // Maintain the sort of the Tuples coming from the workers.
      ParallelStream parallelStream = new ParallelStream(workerZkHost, workerCollection, tupleStream, numWorkers, comp);

      StreamFactory factory = new StreamFactory()
          .withFunctionName("search", CloudSolrStream.class)
          .withFunctionName("parallel", ParallelStream.class)
          .withFunctionName("unique", UniqueStream.class);

      parallelStream.setStreamFactory(factory);
      tupleStream = parallelStream;
    }

    if(sqlVisitor.limit > 0) {
      tupleStream = new LimitStream(tupleStream, sqlVisitor.limit);
    }

    if(sqlVisitor.hasColumnAliases) {
      tupleStream = new SelectStream(tupleStream, sqlVisitor.columnAliases);
    }

    return tupleStream;
  }

  private static StreamComparator[] adjustSorts(List<SortItem> sorts, Bucket[] buckets, Map<String, String> reverseColumnAliases) throws IOException {
    List<FieldComparator> adjustedSorts = new ArrayList();
    Set<String> bucketFields = new HashSet();
    Set<String> sortFields = new HashSet();

    for(SortItem sortItem : sorts) {

      sortFields.add(getSortField(sortItem, reverseColumnAliases));
      adjustedSorts.add(new FieldComparator(getSortField(sortItem, reverseColumnAliases),
                                            ascDescComp(sortItem.getOrdering().toString())));
    }

    for(Bucket bucket : buckets) {
      bucketFields.add(bucket.toString());
    }

    for(SortItem sortItem : sorts) {
      String sortField = getSortField(sortItem, reverseColumnAliases);
      if(!bucketFields.contains(sortField)) {
        throw new IOException("All sort fields must be in the field list.");
      }
    }

    //Add sort fields if needed
    if(sorts.size() < buckets.length) {
      for(Bucket bucket : buckets) {
        String b = bucket.toString();
        if(!sortFields.contains(b)) {
          adjustedSorts.add(new FieldComparator(bucket.toString(), ComparatorOrder.ASCENDING));
        }
      }
    }

    return adjustedSorts.toArray(new FieldComparator[adjustedSorts.size()]);
  }

  private static TupleStream doSelectDistinctFacets(SQLVisitor sqlVisitor) throws IOException {

    Set<String> fieldSet = new HashSet();
    Bucket[] buckets = getBuckets(sqlVisitor.fields, fieldSet);
    Metric[] metrics = getMetrics(sqlVisitor.fields, fieldSet);

    if(metrics.length > 0) {
      throw new IOException("Select Distinct queries cannot include aggregate functions.");
    }

    TableSpec tableSpec = new TableSpec(sqlVisitor.table, defaultZkhost);

    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.Q, sqlVisitor.query);

    int limit = sqlVisitor.limit > 0 ? sqlVisitor.limit : 100;

    FieldComparator[] sorts = null;

    if(sqlVisitor.sorts == null) {
      sorts = new FieldComparator[buckets.length];
      for(int i=0; i<sorts.length; i++) {
        sorts[i] = new FieldComparator("index", ComparatorOrder.ASCENDING);
      }
    } else {
      StreamComparator[] comps = adjustSorts(sqlVisitor.sorts, buckets, sqlVisitor.reverseColumnAliases);
      sorts = new FieldComparator[comps.length];
      for(int i=0; i<comps.length; i++) {
        sorts[i] = (FieldComparator)comps[i];
      }
    }

    TupleStream tupleStream = new FacetStream(zkHost,
                                              collection,
                                              params,
                                              buckets,
                                              metrics,
                                              sorts,
                                              limit);

    if(sqlVisitor.limit > 0) {
      tupleStream = new LimitStream(tupleStream, sqlVisitor.limit);
    }

    return new SelectStream(tupleStream, sqlVisitor.columnAliases);
  }

  private static TupleStream doGroupByWithAggregatesFacets(SQLVisitor sqlVisitor) throws IOException {

    Set<String> fieldSet = new HashSet();
    Bucket[] buckets = getBuckets(sqlVisitor.groupBy, fieldSet);
    Metric[] metrics = getMetrics(sqlVisitor.fields, fieldSet);
    if(metrics.length == 0) {
      throw new IOException("Group by queries must include at least one aggregate function.");
    }

    TableSpec tableSpec = new TableSpec(sqlVisitor.table, defaultZkhost);

    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.Q, sqlVisitor.query);

    int limit = sqlVisitor.limit > 0 ? sqlVisitor.limit : 100;

    FieldComparator[] sorts = null;

    if(sqlVisitor.sorts == null) {
      sorts = new FieldComparator[buckets.length];
      for(int i=0; i<sorts.length; i++) {
        sorts[i] = new FieldComparator("index", ComparatorOrder.ASCENDING);
      }
    } else {
      sorts = getComps(sqlVisitor.sorts, sqlVisitor.reverseColumnAliases);
    }

    TupleStream tupleStream = new FacetStream(zkHost,
                                              collection,
                                              params,
                                              buckets,
                                              metrics,
                                              sorts,
                                              limit);

    if(sqlVisitor.havingExpression != null) {
      tupleStream = new HavingStream(tupleStream, sqlVisitor.havingExpression, sqlVisitor.reverseColumnAliases);
    }

    if(sqlVisitor.limit > 0)
    {
      tupleStream = new LimitStream(tupleStream, sqlVisitor.limit);
    }

    if(sqlVisitor.hasColumnAliases) {
      tupleStream = new SelectStream(tupleStream, sqlVisitor.columnAliases);
    }

    return tupleStream;
  }

  private static TupleStream doSelect(SQLVisitor sqlVisitor) throws IOException {
    List<String> fields = sqlVisitor.fields;
    Set<String> fieldSet = new HashSet();
    Metric[] metrics = getMetrics(fields, fieldSet);
    if(metrics.length > 0) {
      return doAggregates(sqlVisitor, metrics);
    }

    StringBuilder flbuf = new StringBuilder();
    boolean comma = false;

    if(fields.size() == 0) {
      throw new IOException("Select columns must be specified.");
    }

    TableSpec tableSpec = new TableSpec(sqlVisitor.table, defaultZkhost);

    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;

    boolean score = false;

    for (String field : fields) {

      if(field.contains("(")) {
        throw new IOException("Aggregate functions only supported with group by queries.");
      }

      if(field.contains("*")) {
        throw new IOException("* is not supported for column selection.");
      }

      if(field.equals("score")) {
        if(sqlVisitor.limit < 0) {
          throw new IOException("score is not a valid field for unlimited select queries");
        } else {
          score = true;
        }
      }

      if (comma) {
        flbuf.append(",");
      }

      comma = true;
      flbuf.append(field);
    }

    String fl = flbuf.toString();

    List<SortItem> sorts = sqlVisitor.sorts;

    StringBuilder siBuf = new StringBuilder();

    comma = false;

    if(sorts != null) {
      for (SortItem sortItem : sorts) {
        if (comma) {
          siBuf.append(",");
        }
        siBuf.append(getSortField(sortItem, sqlVisitor.reverseColumnAliases) + " " + ascDesc(sortItem.getOrdering().toString()));
      }
    } else {
      if(sqlVisitor.limit < 0) {
        siBuf.append("_version_ desc");
        fl = fl+",_version_";
      } else {
        siBuf.append("score desc");
        if(!score) {
          fl = fl+",score";
        }
      }
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("fl", fl.toString());
    params.set("q", sqlVisitor.query);

    if(siBuf.length() > 0) {
      params.set("sort", siBuf.toString());
    }

    TupleStream tupleStream;

    if(sqlVisitor.limit > -1) {
      params.set("rows", Integer.toString(sqlVisitor.limit));
      tupleStream = new LimitStream(new CloudSolrStream(zkHost, collection, params), sqlVisitor.limit);
    } else {
      //Only use the export handler when no limit is specified.
      params.set(CommonParams.QT, "/export");
      tupleStream = new CloudSolrStream(zkHost, collection, params);
    }

    return new SelectStream(tupleStream, sqlVisitor.columnAliases);
  }

  private static boolean sortsEqual(Bucket[] buckets, String direction, List<SortItem> sortItems, Map<String, String> reverseColumnAliases) {
    if(buckets.length != sortItems.size()) {
      return false;
    }

    for(int i=0; i< buckets.length; i++) {
      Bucket bucket = buckets[i];
      SortItem sortItem = sortItems.get(i);
      if(!bucket.toString().equals(getSortField(sortItem, reverseColumnAliases))) {
        return false;
      }


      if(!sortItem.getOrdering().toString().toLowerCase(Locale.ROOT).contains(direction.toLowerCase(Locale.ROOT))) {
        return false;
      }
    }

    return true;
  }

  private static TupleStream doAggregates(SQLVisitor sqlVisitor, Metric[] metrics) throws IOException {

    if(metrics.length != sqlVisitor.fields.size()) {
      throw new IOException("Only aggregate functions are allowed when group by is not specified.");
    }

    TableSpec tableSpec = new TableSpec(sqlVisitor.table, defaultZkhost);

    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.Q, sqlVisitor.query);

    TupleStream tupleStream = new StatsStream(zkHost,
                                              collection,
                                              params,
                                              metrics);

    if(sqlVisitor.hasColumnAliases) {
      tupleStream = new SelectStream(tupleStream, sqlVisitor.columnAliases);
    }

    return tupleStream;
  }

  private static String bucketSort(Bucket[] buckets, String dir) {
    StringBuilder buf = new StringBuilder();
    boolean comma = false;
    for(Bucket bucket : buckets) {
      if(comma) {
        buf.append(",");
      }
      buf.append(bucket.toString()).append(" ").append(dir);
      comma = true;
    }

    return buf.toString();
  }

  private static String getPartitionKeys(Bucket[] buckets) {
    StringBuilder buf = new StringBuilder();
    boolean comma = false;
    for(Bucket bucket : buckets) {
      if(comma) {
        buf.append(",");
      }
      buf.append(bucket.toString());
      comma = true;
    }
    return buf.toString();
  }

  private static String getSortDirection(List<SortItem> sorts) {
    if(sorts != null && sorts.size() > 0) {
      for(SortItem item : sorts) {
        return ascDesc(stripSingleQuotes(stripQuotes(item.getOrdering().toString())));
      }
    }

    return "asc";
  }

  private static StreamComparator bucketSortComp(Bucket[] buckets, String dir) {
    FieldComparator[] comps = new FieldComparator[buckets.length];
    for(int i=0; i<buckets.length; i++) {
      ComparatorOrder comparatorOrder = ascDescComp(dir);
      String sortKey = buckets[i].toString();
      comps[i] = new FieldComparator(stripQuotes(sortKey), comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private static StreamComparator getComp(List<SortItem> sortItems, Map<String, String> reverseColumnAliases) {
    FieldComparator[] comps = new FieldComparator[sortItems.size()];
    for(int i=0; i<sortItems.size(); i++) {
      SortItem sortItem = sortItems.get(i);
      String ordering = sortItem.getOrdering().toString();
      ComparatorOrder comparatorOrder = ascDescComp(ordering);
      String sortKey = getSortField(sortItem, reverseColumnAliases);
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private static FieldComparator[] getComps(List<SortItem> sortItems, Map<String, String> reverseColumnAliases) {
    FieldComparator[] comps = new FieldComparator[sortItems.size()];
    for(int i=0; i<sortItems.size(); i++) {
      SortItem sortItem = sortItems.get(i);
      String ordering = sortItem.getOrdering().toString();
      ComparatorOrder comparatorOrder = ascDescComp(ordering);
      String sortKey = getSortField(sortItem, reverseColumnAliases);
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    return comps;
  }


  private static String fields(Set<String> fieldSet) {
    StringBuilder buf = new StringBuilder();
    boolean comma = false;
    for(String field : fieldSet) {
      if(comma) {
        buf.append(",");
      }
      buf.append(field);
      comma = true;
    }

    return buf.toString();
  }

  private static Metric[] getMetrics(List<String> fields, Set<String> fieldSet) throws IOException {
    List<Metric> metrics = new ArrayList();
    for(String field : fields) {
      if(field.contains("(")) {

        field = field.substring(0, field.length()-1);
        String[] parts = field.split("\\(");
        String function = parts[0];
        validateFunction(function);
        String column = parts[1];
        if(function.equals("min")) {
          metrics.add(new MinMetric(column));
          fieldSet.add(column);
        } else if(function.equals("max")) {
          metrics.add(new MaxMetric(column));
          fieldSet.add(column);
        } else if(function.equals("sum")) {
          metrics.add(new SumMetric(column));
          fieldSet.add(column);
        } else if(function.equals("avg")) {
          metrics.add(new MeanMetric(column));
          fieldSet.add(column);
        } else if(function.equals("count")) {
          metrics.add(new CountMetric());
        }
      }
    }
    return metrics.toArray(new Metric[metrics.size()]);
  }

  private static void validateFunction(String function) throws IOException {
    if(function.equals("min") || function.equals("max") || function.equals("sum") || function.equals("avg") || function.equals("count")) {
      return;
    } else {
      throw new IOException("Invalid function: "+function);
    }
  }

  private static Bucket[] getBuckets(List<String> fields, Set<String> fieldSet) {
    List<Bucket> buckets = new ArrayList();
    for(String field : fields) {
      String f = stripQuotes(field);
      buckets.add(new Bucket(f));
      fieldSet.add(f);
    }

    return buckets.toArray(new Bucket[buckets.size()]);
  }

  private static String ascDesc(String s) {
    if(s.toLowerCase(Locale.ROOT).contains("desc")) {
      return "desc";
    } else {
      return "asc";
    }
  }

  private static ComparatorOrder ascDescComp(String s) {
    if(s.toLowerCase(Locale.ROOT).contains("desc")) {
      return ComparatorOrder.DESCENDING;
    } else {
      return ComparatorOrder.ASCENDING;
    }
  }

  private static String stripQuotes(String s) {
    StringBuilder buf = new StringBuilder();
    for(int i=0; i<s.length(); i++) {
      char c = s.charAt(i);
      if(c != '"') {
        buf.append(c);
      }
    }

    return buf.toString();
  }

  private static String stripSingleQuotes(String s) {
    StringBuilder buf = new StringBuilder();
    for(int i=0; i<s.length(); i++) {
      char c = s.charAt(i);
      if(c != '\'') {
        buf.append(c);
      }
    }

    return buf.toString();
  }


  private static class TableSpec {
    private String collection;
    private String zkHost;

    public TableSpec(String table, String defaultZkHost) {
      if(table.contains("@")) {
        String[] parts = table.split("@");
        this.collection = parts[0];
        this.zkHost = parts[1];
      } else {
        this.collection = table;
        this.zkHost = defaultZkHost;
      }
    }
  }

  private static class ExpressionVisitor extends AstVisitor<Void, StringBuilder> {

    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, StringBuilder buf) {
      buf.append("(");
      process(node.getLeft(), buf);
      buf.append(" ").append(node.getType().toString()).append(" ");
      process(node.getRight(), buf);
      buf.append(")");
      return null;
    }

    protected Void visitNotExpression(NotExpression node, StringBuilder buf) {
      buf.append("-");
      process(node.getValue(), buf);
      return null;
    }

    protected Void visitComparisonExpression(ComparisonExpression node, StringBuilder buf) {
      if (!(node.getLeft() instanceof StringLiteral || node.getLeft() instanceof QualifiedNameReference)) {
        throw new RuntimeException("Left side of comparison must be a literal.");
      }

      String field = getPredicateField(node.getLeft());
      String value = node.getRight().toString();
      value = stripSingleQuotes(value);

      if(!value.startsWith("(") && !value.startsWith("[")) {
        //If no parens default to a phrase search.
        value = '"'+value+'"';
      }

      String lowerBound;
      String upperBound;
      String lowerValue;
      String upperValue;

      ComparisonExpression.Type t = node.getType();
      switch(t) {
        case NOT_EQUAL:
          buf.append('(').append('-').append(field).append(":").append(value).append(')');
          return null;
        case EQUAL:
          buf.append('(').append(field).append(":").append(value).append(')');
          return null;
        case LESS_THAN:
          lowerBound = "[";
          upperBound = "}";
          lowerValue = "*";
          upperValue = value;
          buf.append('(').append(field).append(":").append(lowerBound).append(lowerValue).append(" TO ").append(upperValue).append(upperBound).append(')');
          return null;
        case LESS_THAN_OR_EQUAL:
          lowerBound = "[";
          upperBound = "]";
          lowerValue = "*";
          upperValue = value;
          buf.append('(').append(field).append(":").append(lowerBound).append(lowerValue).append(" TO ").append(upperValue).append(upperBound).append(')');
          return null;
        case GREATER_THAN:
          lowerBound = "{";
          upperBound = "]";
          lowerValue = value;
          upperValue = "*";
          buf.append('(').append(field).append(":").append(lowerBound).append(lowerValue).append(" TO ").append(upperValue).append(upperBound).append(')');
          return null;
        case GREATER_THAN_OR_EQUAL:
          lowerBound = "[";
          upperBound = "]";
          lowerValue = value;
          upperValue = "*";
          buf.append('(').append(field).append(":").append(lowerBound).append(lowerValue).append(" TO ").append(upperValue).append(upperBound).append(')');
          return null;
      }

      return null;
    }
  }

  static class SQLVisitor extends AstVisitor<Void, Integer> {
    private final StringBuilder builder;
    public String table;
    public List<String> fields = new ArrayList();
    public List<String> groupBy = new ArrayList();
    public List<SortItem> sorts;
    public String query ="*:*"; //If no query is specified pull all the records
    public int limit = -1;
    public boolean groupByQuery;
    public Expression havingExpression;
    public boolean isDistinct;
    public boolean hasColumnAliases;
    public Map<String, String> columnAliases = new HashMap();
    public Map<String, String> reverseColumnAliases = new HashMap();

    public SQLVisitor(StringBuilder builder) {
      this.builder = builder;
    }

    protected Void visitNode(Node node, Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    protected void reverseAliases() {
      for(String key : columnAliases.keySet()) {
        reverseColumnAliases.put(columnAliases.get(key), key);
      }

      //Handle the group by.
      List<String> newGroups = new ArrayList();

      for(String g : groupBy) {
        if (reverseColumnAliases.containsKey(g)) {
          newGroups.add(reverseColumnAliases.get(g));
        } else {
          newGroups.add(g);
        }
      }

      groupBy = newGroups;
    }




    protected Void visitUnnest(Unnest node, Integer indent) {
      return null;
    }

    protected Void visitQuery(Query node, Integer indent) {
      if(node.getWith().isPresent()) {
        With confidence = (With)node.getWith().get();
        this.append(indent.intValue(), "WITH");
        if(confidence.isRecursive()) {
        }

        Iterator queries = confidence.getQueries().iterator();

        while(queries.hasNext()) {
          WithQuery query = (WithQuery)queries.next();
          this.process(new TableSubquery(query.getQuery()), indent);
          if(queries.hasNext()) {
          }
        }
      }

      this.processRelation(node.getQueryBody(), indent);
      if(!node.getOrderBy().isEmpty()) {
        this.sorts = node.getOrderBy();
      }

      if(node.getLimit().isPresent()) {
      }

      if(node.getApproximate().isPresent()) {

      }

      return null;
    }

    protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
      this.process(node.getSelect(), indent);
      if(node.getFrom().isPresent()) {
        this.process((Node)node.getFrom().get(), indent);
      }

      if(node.getWhere().isPresent()) {
        Expression ex  = node.getWhere().get();
        ExpressionVisitor expressionVisitor = new ExpressionVisitor();
        StringBuilder buf = new StringBuilder();
        expressionVisitor.process(ex, buf);
        this.query = buf.toString();
      }

      if(!node.getGroupBy().isEmpty()) {
        this.groupByQuery = true;
        List<Expression> groups = node.getGroupBy();
        for(Expression group : groups) {
          groupBy.add(getGroupField(group));
        }
      }

      if(node.getHaving().isPresent()) {
        this.havingExpression = node.getHaving().get();
      }

      if(!node.getOrderBy().isEmpty()) {
        this.sorts = node.getOrderBy();
      }

      if(node.getLimit().isPresent()) {
        this.limit = Integer.parseInt(stripQuotes(node.getLimit().get()));
      }

      return null;
    }

    protected Void visitComparisonExpression(ComparisonExpression node, Integer index) {
      String field = node.getLeft().toString();
      String value = node.getRight().toString();
      query = stripSingleQuotes(stripQuotes(field))+":"+stripQuotes(value);
      return null;
    }

    protected Void visitSelect(Select node, Integer indent) {
      this.append(indent.intValue(), "SELECT");
      if(node.isDistinct()) {
        this.isDistinct = true;
      }

      if(node.getSelectItems().size() > 1) {
        boolean first = true;

        for(Iterator var4 = node.getSelectItems().iterator(); var4.hasNext(); first = false) {
          SelectItem item = (SelectItem)var4.next();
          this.process(item, indent);
        }
      } else {
        this.process((Node) Iterables.getOnlyElement(node.getSelectItems()), indent);
      }

      return null;
    }

    protected Void visitSingleColumn(SingleColumn node, Integer indent) {

      Expression ex = node.getExpression();
      String field = null;

      if(ex instanceof QualifiedNameReference) {

        QualifiedNameReference ref = (QualifiedNameReference)ex;
        List<String> parts = ref.getName().getOriginalParts();
        field = parts.get(0);

      } else if(ex instanceof  FunctionCall) {

        FunctionCall functionCall = (FunctionCall)ex;
        List<String> parts = functionCall.getName().getOriginalParts();
        List<Expression> args = functionCall.getArguments();
        String col = null;

        if(args.size() > 0 && args.get(0) instanceof QualifiedNameReference) {
          QualifiedNameReference ref = (QualifiedNameReference) args.get(0);
          col = ref.getName().getOriginalParts().get(0);
          field = parts.get(0)+"("+stripSingleQuotes(col)+")";
        } else {
          field = stripSingleQuotes(stripQuotes(functionCall.toString()));
        }

      } else if(ex instanceof StringLiteral) {
        StringLiteral stringLiteral = (StringLiteral)ex;
        field = stripSingleQuotes(stringLiteral.toString());
      }

      fields.add(field);

      if(node.getAlias().isPresent()) {
        String alias = node.getAlias().get();
        columnAliases.put(field, alias);
        hasColumnAliases = true;
      } else {
        columnAliases.put(field, field);
      }

      return null;
    }




    protected Void visitAllColumns(AllColumns node, Integer context) {
      return null;
    }

    protected Void visitTable(Table node, Integer indent) {
      this.table = stripSingleQuotes(node.getName().toString());
      return null;
    }

    protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
      this.process(node.getRelation(), indent);
      return null;
    }

    protected Void visitValues(Values node, Integer indent) {
      boolean first = true;

      for(Iterator var4 = node.getRows().iterator(); var4.hasNext(); first = false) {
        Expression row = (Expression)var4.next();

      }

      return null;
    }

    private void processRelation(Relation relation, Integer indent) {
      if(relation instanceof Table) {
      } else {
        this.process(relation, indent);
      }
    }

    private StringBuilder append(int indent, String value) {
      return this.builder.append(indentString(indent)).append(value);
    }

    private static String indentString(int indent) {
      return Strings.repeat("   ", indent);
    }
  }

  private static String getSortField(SortItem sortItem, Map<String, String> reverseColumnAliases)
  {
    String field;
    Expression ex = sortItem.getSortKey();
    if(ex instanceof QualifiedNameReference) {
      QualifiedNameReference ref = (QualifiedNameReference)ex;
      List<String> parts = ref.getName().getOriginalParts();
      field = parts.get(0);
    } else if(ex instanceof  FunctionCall) {
      FunctionCall functionCall = (FunctionCall)ex;
      List<String> parts = functionCall.getName().getOriginalParts();
      List<Expression> args = functionCall.getArguments();
      String col = null;

      if(args.size() > 0 && args.get(0) instanceof QualifiedNameReference) {
        QualifiedNameReference ref = (QualifiedNameReference) args.get(0);
        col = ref.getName().getOriginalParts().get(0);
        field = parts.get(0)+"("+stripSingleQuotes(col)+")";
      } else {
        field = stripSingleQuotes(stripQuotes(functionCall.toString()));
      }

    } else {
      StringLiteral stringLiteral = (StringLiteral)ex;
      field = stripSingleQuotes(stringLiteral.toString());
    }

    if(reverseColumnAliases.containsKey(field)) {
      field = reverseColumnAliases.get(field);
    }

    return field;
  }


  private static String getHavingField(Expression ex)
  {
    String field;
    if(ex instanceof QualifiedNameReference) {
      QualifiedNameReference ref = (QualifiedNameReference)ex;
      List<String> parts = ref.getName().getOriginalParts();
      field = parts.get(0);
    } else if(ex instanceof  FunctionCall) {
      FunctionCall functionCall = (FunctionCall)ex;
      List<String> parts = functionCall.getName().getOriginalParts();
      List<Expression> args = functionCall.getArguments();
      String col = null;

      if(args.size() > 0 && args.get(0) instanceof QualifiedNameReference) {
        QualifiedNameReference ref = (QualifiedNameReference) args.get(0);
        col = ref.getName().getOriginalParts().get(0);
        field = parts.get(0)+"("+stripSingleQuotes(col)+")";
      } else {
        field = stripSingleQuotes(stripQuotes(functionCall.toString()));
      }

    } else {
      StringLiteral stringLiteral = (StringLiteral)ex;
      field = stripSingleQuotes(stringLiteral.toString());
    }

    return field;
  }


  private static String getPredicateField(Expression ex)
  {
    String field;
    if(ex instanceof QualifiedNameReference) {
      QualifiedNameReference ref = (QualifiedNameReference)ex;
      List<String> parts = ref.getName().getOriginalParts();
      field = parts.get(0);
    } else {
      StringLiteral stringLiteral = (StringLiteral)ex;
      field = stripSingleQuotes(stringLiteral.toString());
    }

    return field;
  }

  private static String getGroupField(Expression ex)
  {
    String field;
    if(ex instanceof QualifiedNameReference) {
      QualifiedNameReference ref = (QualifiedNameReference)ex;
      List<String> parts = ref.getName().getOriginalParts();
      field = parts.get(0);
    } else {
      StringLiteral stringLiteral = (StringLiteral)ex;
      field = stripSingleQuotes(stringLiteral.toString());
    }

    return field;
  }


  private static class LimitStream extends TupleStream {

    private TupleStream stream;
    private int limit;
    private int count;

    public LimitStream(TupleStream stream, int limit) {
      this.stream = stream;
      this.limit = limit;
    }

    public void open() throws IOException {
      this.stream.open();
    }

    public void close() throws IOException {
      this.stream.close();
    }

    public List<TupleStream> children() {
      List<TupleStream> children = new ArrayList();
      children.add(stream);
      return children;
    }

    public StreamComparator getStreamSort(){
      return stream.getStreamSort();
    }

    public void setStreamContext(StreamContext context) {
      stream.setStreamContext(context);
    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
          stream.toExplanation(factory)
        })
        .withFunctionName("SQL LIMIT")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR);
    }

    public Tuple read() throws IOException {
      ++count;
      if(count > limit) {
        Map fields = new HashMap();
        fields.put("EOF", "true");
        return new Tuple(fields);
      }

      Tuple tuple = stream.read();
      return tuple;
    }
  }

  public static enum AggregationMode {

    MAP_REDUCE,
    FACET;

    public static AggregationMode getMode(String mode) throws IOException{
      if(mode.equalsIgnoreCase("facet")) {
        return FACET;
      } else if(mode.equalsIgnoreCase("map_reduce")) {
        return MAP_REDUCE;
      } else {
        throw new IOException("Invalid aggregation mode:"+mode);
      }
    }
  }

  private static class HavingStream extends TupleStream {

    private TupleStream stream;
    private HavingVisitor havingVisitor;
    private Expression havingExpression;

    public HavingStream(TupleStream stream, Expression havingExpression, Map<String, String> reverseAliasMap) {
      this.stream = stream;
      this.havingVisitor = new HavingVisitor(reverseAliasMap);
      this.havingExpression = havingExpression;
    }

    public void open() throws IOException {
      this.stream.open();
    }

    public void close() throws IOException {
      this.stream.close();
    }

    public StreamComparator getStreamSort(){
      return stream.getStreamSort();
    }

    public List<TupleStream> children() {
      List<TupleStream> children = new ArrayList();
      children.add(stream);
      return children;
    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
          stream.toExplanation(factory)
        })
        .withFunctionName("SQL HAVING")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR);
    }

    public void setStreamContext(StreamContext context) {
      stream.setStreamContext(context);
    }

    public Tuple read() throws IOException {
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) {
          return tuple;
        }

        if (havingVisitor.process(havingExpression, tuple)) {
          return tuple;
        }
      }
    }
  }

  private static class CatalogsStream extends TupleStream {
    private final String zkHost;
    private StreamContext context;
    private int currentIndex = 0;
    private List<String> catalogs;

    CatalogsStream(String zkHost) {
      this.zkHost = zkHost;
    }

    public List<TupleStream> children() {
      return new ArrayList<>();
    }

    public void open() throws IOException {
      this.catalogs = new ArrayList<>();
      this.catalogs.add(this.zkHost);
    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("SQL CATALOG")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR);
    }

    public Tuple read() throws IOException {
      Map<String, String> fields = new HashMap<>();
      if (this.currentIndex < this.catalogs.size()) {
        fields.put("TABLE_CAT", this.catalogs.get(this.currentIndex));
        this.currentIndex += 1;
      } else {
        fields.put("EOF", "true");
      }
      return new Tuple(fields);
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() throws IOException {

    }

    public void setStreamContext(StreamContext context) {
      this.context = context;
    }
  }

  private static class SchemasStream extends TupleStream {
    private final String zkHost;
    private StreamContext context;

    SchemasStream(String zkHost) {
      this.zkHost = zkHost;
    }

    public List<TupleStream> children() {
      return new ArrayList<>();
    }

    public void open() throws IOException {

    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("SQL SCHEMA")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR);
    }

    public Tuple read() throws IOException {
      Map<String, String> fields = new HashMap<>();
      fields.put("EOF", "true");
      return new Tuple(fields);
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() throws IOException {

    }

    public void setStreamContext(StreamContext context) {
      this.context = context;
    }
  }

  private static class TableStream extends TupleStream {
    private final String zkHost;
    private StreamContext context;
    private int currentIndex = 0;
    private List<String> tables;

    TableStream(String zkHost) {
      this.zkHost = zkHost;
    }

    public List<TupleStream> children() {
      return new ArrayList<>();
    }

    public void open() throws IOException {
      this.tables = new ArrayList<>();

      CloudSolrClient cloudSolrClient = this.context.getSolrClientCache().getCloudSolrClient(this.zkHost);
      cloudSolrClient.connect();
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      Set<String> collections = zkStateReader.getClusterState().getCollectionStates().keySet();
      if (collections.size() != 0) {
        this.tables.addAll(collections);
      }
      Collections.sort(this.tables);
    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("SQL TABLE")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR);
    }

    public Tuple read() throws IOException {
      Map<String, String> fields = new HashMap<>();
      if (this.currentIndex < this.tables.size()) {
        fields.put("TABLE_CAT", this.zkHost);
        fields.put("TABLE_SCHEM", null);
        fields.put("TABLE_NAME", this.tables.get(this.currentIndex));
        fields.put("TABLE_TYPE", "TABLE");
        fields.put("REMARKS", null);
        this.currentIndex += 1;
      } else {
        fields.put("EOF", "true");
      }
      return new Tuple(fields);
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() throws IOException {

    }

    public void setStreamContext(StreamContext context) {
      this.context = context;
    }
  }

  private static class MetadataStream extends TupleStream {

    private final TupleStream stream;
    private final SQLVisitor sqlVisitor;
    private boolean firstTuple = true;

    public MetadataStream(TupleStream stream, SQLVisitor sqlVistor) {
      this.stream = stream;
      this.sqlVisitor = sqlVistor;
    }

    public List<TupleStream> children() {
      return this.stream.children();
    }

    public void open() throws IOException {
      this.stream.open();
    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
          stream.toExplanation(factory)
        })
        .withFunctionName("SQL METADATA")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR);
    }

    // Return a metadata tuple as the first tuple and then pass through to the underlying stream.
    public Tuple read() throws IOException {
      if(firstTuple) {
        firstTuple = false;

        Map fields = new HashMap<>();
        fields.put("isMetadata", true);
        fields.put("fields", sqlVisitor.fields);
        fields.put("aliases", sqlVisitor.columnAliases);
        return new Tuple(fields);
      }

      return this.stream.read();
    }

    public StreamComparator getStreamSort() {
      return this.stream.getStreamSort();
    }

    public void close() throws IOException {
      this.stream.close();
    }

    public void setStreamContext(StreamContext context) {
      this.stream.setStreamContext(context);
    }
  }

  private static class HavingVisitor extends AstVisitor<Boolean, Tuple> {

    private Map<String,String> reverseAliasMap;

    public HavingVisitor(Map<String, String> reverseAliasMap) {
      this.reverseAliasMap = reverseAliasMap;
    }

    protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression node, Tuple tuple) {

      Boolean b = process(node.getLeft(), tuple);
      if(node.getType() == LogicalBinaryExpression.Type.AND) {
        if(!b) {
          //Short circuit
          return false;
        } else {
          return process(node.getRight(), tuple);
        }
      } else {
        if(b) {
          //Short circuit
         return true;
        } else {
          return process(node.getRight(), tuple);
        }
      }
    }

    protected Boolean visitComparisonExpression(ComparisonExpression node, Tuple tuple) {
      String field = getHavingField(node.getLeft());

      if(reverseAliasMap.containsKey(field)) {
        field = reverseAliasMap.get(field);
      }

      double d = Double.parseDouble(node.getRight().toString());
      double td = tuple.getDouble(field);
      ComparisonExpression.Type t = node.getType();

      switch(t) {
        case LESS_THAN:
          return td < d;
        case LESS_THAN_OR_EQUAL:
          return td <= d;
        case NOT_EQUAL:
          return td != d;
        case EQUAL:
          return td == d;
        case GREATER_THAN:
          return td > d;
        case GREATER_THAN_OR_EQUAL:
          return td >= d;
        default:
          return false;
      }
    }
  }
 }
