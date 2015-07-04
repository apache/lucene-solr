package org.apache.solr.handler;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.*;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.ParallelStream;
import org.apache.solr.client.solrj.io.stream.RankStream;
import org.apache.solr.client.solrj.io.stream.RollupStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.facebook.presto.sql.parser.SqlParser;

public class SQLHandler extends RequestHandlerBase implements SolrCoreAware {

  private Map<String, TableSpec> tableMappings = new HashMap();
  private String defaultZkhost = null;
  private String defaultWorkerCollection = null;

  private Logger logger = LoggerFactory.getLogger(SQLHandler.class);

  public void inform(SolrCore core) {

    CoreContainer coreContainer = core.getCoreDescriptor().getCoreContainer();

    if(coreContainer.isZooKeeperAware()) {
      defaultZkhost = core.getCoreDescriptor().getCoreContainer().getZkController().getZkServerAddress();
      defaultWorkerCollection = core.getCoreDescriptor().getCollectionName();
    }

    NamedList<String> tableConf = (NamedList<String>)initArgs.get("tables");

    for(Entry<String,String> entry : tableConf) {
      String tableName = entry.getKey();
      if(entry.getValue().indexOf("@") > -1) {
        String[] parts = entry.getValue().split("@");
        tableMappings.put(tableName, new TableSpec(parts[0], parts[1]));
      } else {
        String collection = entry.getValue();
        tableMappings.put(tableName, new TableSpec(collection, defaultZkhost));
      }
    }
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    String sql = params.get("sql");
    int numWorkers = params.getInt("numWorkers", 1);
    String workerCollection = params.get("workerCollection", defaultWorkerCollection);
    String workerZkhost = params.get("workerZkhost",defaultZkhost);
    StreamContext context = new StreamContext();
    try {
      TupleStream tupleStream = SQLTupleStreamParser.parse(sql, tableMappings, numWorkers, workerCollection, workerZkhost);
      context.numWorkers = numWorkers;
      context.setSolrClientCache(StreamHandler.clientCache);
      tupleStream.setStreamContext(context);
      rsp.add("tuples", new ExceptionStream(tupleStream));
    } catch(Exception e) {
      //Catch the SQL parsing and query transformation exceptions.
      logger.error("Exception parsing SQL", e);
      rsp.add("tuples", new StreamHandler.DummyErrorStream(e));
    }
  }

  public String getDescription() {
    return "SQLHandler";
  }

  public String getSource() {
    return null;
  }

  public static class SQLTupleStreamParser {

    public static TupleStream parse(String sql,
                                    Map<String, TableSpec> tableMap,
                                    int numWorkers,
                                    String workerCollection,
                                    String workerZkhost) throws IOException {
      SqlParser parser = new SqlParser();
      Statement statement = parser.createStatement(sql);

      SQLVisitor sqlVistor = new SQLVisitor(new StringBuilder());

      sqlVistor.process(statement, new Integer(0));

      TupleStream sqlStream = null;

      if(sqlVistor.groupByQuery) {
        sqlStream = doGroupBy(sqlVistor, tableMap, numWorkers, workerCollection, workerZkhost);
      } else {
        sqlStream = doSelect(sqlVistor, tableMap, numWorkers, workerCollection, workerZkhost);
      }

      return sqlStream;
    }
  }

  private static TupleStream doGroupBy(SQLVisitor sqlVisitor,
                                       Map<String, TableSpec> tableMap,
                                       int numWorkers,
                                       String workerCollection,
                                       String workerZkHost) throws IOException {

    Set<String> fieldSet = new HashSet();
    Bucket[] buckets = getBuckets(sqlVisitor.groupBy, fieldSet);
    Metric[] metrics = getMetrics(sqlVisitor.fields, fieldSet);

    String fl = fields(fieldSet);
    String sortDirection = getSortDirection(sqlVisitor.sorts);
    String sort = bucketSort(buckets, sortDirection);

    TableSpec tableSpec = tableMap.get(sqlVisitor.table);
    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    Map<String, String> params = new HashMap();

    params.put(CommonParams.FL, fl);
    params.put(CommonParams.Q, sqlVisitor.query);
    //Always use the /export handler for Group By Queries because it requires exporting full result sets.
    params.put(CommonParams.QT, "/export");

    if(numWorkers > 1) {
      params.put("partitionKeys", getPartitionKeys(buckets));
    }

    params.put("sort", sort);

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
      parallelStream.setObjectSerialize(false);
      tupleStream = parallelStream;
    }

    //TODO: This should be done on the workers, but it won't serialize because it relies on Presto classes.
    // Once we make this a Expressionable the problem will be solved.

    if(sqlVisitor.havingExpression != null) {
      tupleStream = new HavingStream(tupleStream, sqlVisitor.havingExpression);
    }

    if(sqlVisitor.sorts != null && sqlVisitor.sorts.size() > 0) {
      if(!sortsEqual(buckets, sortDirection, sqlVisitor.sorts)) {
        int limit = sqlVisitor.limit == -1 ? 100 : sqlVisitor.limit;
        StreamComparator comp = getComp(sqlVisitor.sorts);
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

    return tupleStream;
  }

  private static TupleStream doSelect(SQLVisitor sqlVisitor,
                                      Map<String, TableSpec> tableMap,
                                      int numWorkers,
                                      String workerCollection,
                                      String workerZkHost) throws IOException {
    List<String> fields = sqlVisitor.fields;
    StringBuilder flbuf = new StringBuilder();
    boolean comma = false;
    for(String field : fields) {

      if(comma) {
        flbuf.append(",");
      }

      comma = true;
      flbuf.append(field);
    }

    String fl = flbuf.toString();

    List<SortItem> sorts = sqlVisitor.sorts;

    StringBuilder siBuf = new StringBuilder();

    comma = false;
    for(SortItem sortItem : sorts) {
      if(comma) {
        siBuf.append(",");
      }
      siBuf.append(stripQuotes(sortItem.getSortKey().toString()) + " " + ascDesc(sortItem.getOrdering().toString()));
    }

    TableSpec tableSpec = tableMap.get(sqlVisitor.table);
    String zkHost = tableSpec.zkHost;
    String collection = tableSpec.collection;
    Map<String, String> params = new HashMap();

    params.put("fl", fl.toString());
    params.put("q", sqlVisitor.query);
    params.put("sort", siBuf.toString());

    if(sqlVisitor.limit > -1) {
      params.put("rows", Integer.toString(sqlVisitor.limit));
      return new LimitStream(new CloudSolrStream(zkHost, collection, params), sqlVisitor.limit);
    } else {
      //Only use the export handler when no limit is specified.
      params.put(CommonParams.QT, "/export");
      return new CloudSolrStream(zkHost, collection, params);
    }
  }

  private static boolean sortsEqual(Bucket[] buckets, String direction, List<SortItem> sortItems) {
    if(buckets.length != sortItems.size()) {
      return false;
    }

    for(int i=0; i< buckets.length; i++) {
      Bucket bucket = buckets[i];
      SortItem sortItem = sortItems.get(i);
      if(!bucket.toString().equals(stripQuotes(sortItem.getSortKey().toString()))) {
        return false;
      }


      if(!sortItem.getOrdering().toString().toLowerCase(Locale.getDefault()).contains(direction.toLowerCase(Locale.getDefault()))) {
        return false;
      }
    }

    return true;
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

  public static String getSortDirection(List<SortItem> sorts) {
    if(sorts != null && sorts.size() > 0) {
      for(SortItem item : sorts) {
        return ascDesc(stripQuotes(item.getOrdering().toString()));
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

  private static StreamComparator getComp(List<SortItem> sortItems) {
    FieldComparator[] comps = new FieldComparator[sortItems.size()];
    for(int i=0; i<sortItems.size(); i++) {
      SortItem sortItem = sortItems.get(i);
      String ordering = sortItem.getOrdering().toString();
      ComparatorOrder comparatorOrder = ascDescComp(ordering);
      String sortKey = sortItem.getSortKey().toString();
      comps[i] = new FieldComparator(stripQuotes(sortKey), comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
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

  private static Metric[] getMetrics(List<String> fields, Set<String> fieldSet) {
    List<Metric> metrics = new ArrayList();
    for(String field : fields) {

      if(field.contains("(")) {

        field = field.substring(0, field.length()-1);
        String[] parts = field.split("\\(");
        String function = parts[0];
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
    if(s.toLowerCase(Locale.getDefault()).contains("desc")) {
      return "desc";
    } else {
      return "asc";
    }
  }

  private static ComparatorOrder ascDescComp(String s) {
    if(s.toLowerCase(Locale.getDefault()).contains("desc")) {
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


  private class TableSpec {
    private String collection;
    private String zkHost;

    public TableSpec(String collection, String zkHost) {
      this.collection = collection;
      this.zkHost = zkHost;
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
      String field = node.getLeft().toString();
      String value = node.getRight().toString();
      buf.append('(').append(stripQuotes(field) + ":" + stripSingleQuotes(value)).append(')');
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

    public SQLVisitor(StringBuilder builder) {
      this.builder = builder;
    }

    protected Void visitNode(Node node, Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
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
          groupBy.add(stripQuotes(group.toString()));

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
      query = stripQuotes(field)+":"+stripQuotes(value);
      return null;
    }

    protected Void visitSelect(Select node, Integer indent) {
      this.append(indent.intValue(), "SELECT");
      if(node.isDistinct()) {

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
      fields.add(stripQuotes(ExpressionFormatter.formatExpression(node.getExpression())));

      if(node.getAlias().isPresent()) {
      }

      return null;
    }

    protected Void visitAllColumns(AllColumns node, Integer context) {
      return null;
    }

    protected Void visitTable(Table node, Integer indent) {
      this.table = node.getName().toString();
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

  public static class HavingStream extends TupleStream {

    private TupleStream stream;
    private HavingVisitor havingVisitor;
    private Expression havingExpression;

    public HavingStream(TupleStream stream, Expression havingExpression) {
      this.stream = stream;
      this.havingVisitor = new HavingVisitor();
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

  private static class HavingVisitor extends AstVisitor<Boolean, Tuple> {

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
      String field = stripQuotes(node.getLeft().toString());
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
          return td <= d;
        case GREATER_THAN_OR_EQUAL:
          return td <= d;
        default:
          return false;
      }
    }
  }
 }
