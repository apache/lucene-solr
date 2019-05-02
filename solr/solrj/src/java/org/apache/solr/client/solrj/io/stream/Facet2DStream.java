package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

public class Facet2DStream extends TupleStream implements Expressible {

  private String collection;
  private ModifiableSolrParams params;
  private Bucket x;
  private Bucket y;
  private String dimensions;
  private Metric metric;
  private String zkHost;

  private List<Tuple> tuples = new ArrayList<Tuple>();
  private int dimensionX;
  private int dimensionY;
  private FieldComparator bucketSort;
  private boolean resortNeeded;
  private boolean refine;
  private int index;

  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;


  public Facet2DStream(StreamExpression expression, StreamFactory factory) throws IOException {

    String collectionName = factory.getValueOperand(expression, 0);

    if (collectionName.indexOf('"') > -1) {
      collectionName = collectionName.replaceAll("\"", "").replaceAll(" ", "");
    }

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter bucketXExpression = factory.getNamedOperand(expression, "x");
    StreamExpressionNamedParameter bucketYExpression = factory.getNamedOperand(expression, "y");
    StreamExpressionNamedParameter dimensionsExpression = factory.getNamedOperand(expression, "dimensions");
    StreamExpression metricExpression = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class).get(0);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    StreamExpressionNamedParameter refineExpression = factory.getNamedOperand(expression, "refine");

    if (collectionName == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - collectionName expected as first operand", expression));
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    for (StreamExpressionNamedParameter namedParam : namedParams) {
      if (!namedParam.getName().equals("x") && !namedParam.getName().equals("y") &&
          !namedParam.getName().equals("dimensions") && !namedParam.getName().equals("zkHost") &&
          !namedParam.getName().equals("refine")) {
        params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }
    Bucket x = null;
    if (bucketXExpression != null) {
      if (bucketXExpression.getParameter() instanceof StreamExpressionValue) {
        String keyX = ((StreamExpressionValue) bucketXExpression.getParameter()).getValue();
        x = new Bucket(keyX.trim());
      }
    }
    Bucket y = null;
    if (bucketYExpression != null) {
      if (bucketYExpression.getParameter() instanceof StreamExpressionValue) {
        String keyY = ((StreamExpressionValue) bucketYExpression.getParameter()).getValue();
        y = new Bucket(keyY.trim());
      }
    }

    if (x == null || y == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - x and y buckets expected. eg. 'x=\"name\"'", expression, collectionName));
    }
    String bucketSortString = (StreamExpressionValue) metricExpression.getParameters().get(0) + " desc";

    FieldComparator bucketSort = parseBucketSort(bucketSortString, x, y);

    Metric metric = factory.constructMetric(metricExpression);
    if (metric == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - one metric expected", expression, collectionName));
    }

    boolean refine = false;

    if (refineExpression != null) {
      String refineStr = ((StreamExpressionValue) refineExpression.getParameter()).getValue();
      if (refineStr != null) {
        refine = Boolean.parseBoolean(refineStr);
      }
    }

    if (dimensionsExpression != null) {
      if (dimensionsExpression.getParameter() instanceof StreamExpressionValue) {
        String[] strDimensions = ((StreamExpressionValue) bucketXExpression.getParameter()).getValue().split(",");
        if (strDimensions.length != 2) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - two dimension values expected"));
        }
        dimensionX = Integer.parseInt(strDimensions[0]);
        dimensionY = Integer.parseInt(strDimensions[1]);
      }
    }

    String zkHost = null;
    if (zkHostExpression == null) {
      zkHost = factory.getCollectionZkHost(collectionName);
      if (zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    } else if (zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue) zkHostExpression.getParameter()).getValue();
    }

    if (zkHost == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - zkHost not found for collection '%s'", expression, collectionName));
    }

    init(collectionName, params, x, y, bucketSort, dimensionX, dimensionY, metric);
  }

  private FieldComparator parseBucketSort(String bucketSortString, Bucket x, Bucket y) throws IOException {
    String[] spec = bucketSortString.trim().split("\\s+");
    String fieldName = spec[0].trim();
    String order = spec[1].trim();
    return new FieldComparator(fieldName, ComparatorOrder.DESCENDING);

  }

  private void init(String collection, SolrParams params, Bucket x, Bucket y, FieldComparator bucketSort, int dimensionX, int dimensionY, Metric metric) {
    this.collection = collection;
    this.params = new ModifiableSolrParams(params);
    this.x = x;
    this.y = y;
    this.dimensionX = dimensionX;
    this.dimensionY = dimensionY;
    this.metric = metric;
    this.bucketSort = bucketSort;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

    child.setExpression(params.stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), Arrays.toString(e.getValue()))).collect(Collectors.joining(",")));
    explanation.addChild(child);

    return explanation;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    if (collection.indexOf(',') > -1) {
      expression.addParameter("\"" + collection + "\"");
    } else {
      expression.addParameter(collection);
    }

    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), val));
      }
    }
    {
      StringBuilder builder = new StringBuilder();
      builder.append(x.toString());
      expression.addParameter(new StreamExpressionNamedParameter("x", builder.toString()));
    }
    {
      StringBuilder builder = new StringBuilder();
      builder.append(y.toString());
      expression.addParameter(new StreamExpressionNamedParameter("y", builder.toString()));
    }

    expression.addParameter(metric.toExpression(factory));
    expression.addParameter(new StreamExpressionNamedParameter("dimensions", Integer.toString(dimensionX) + "," + Integer.toString(dimensionY)));

    return expression;
  }

  public Facet2DStream(String collection, ModifiableSolrParams params, Bucket x, Bucket y, String dimensions, Metric metric) throws IOException {
    if (dimensions != null) {
        String[] strDimensions = dimensions.split(",");
        if (strDimensions.length != 2) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - two dimension values expected"));
        }
        dimensionX = Integer.parseInt(strDimensions[0]);
        dimensionY = Integer.parseInt(strDimensions[1]);

    }
    String bucketSortString = metric.getValue() + " desc";
    FieldComparator bucketSort = parseBucketSort(bucketSortString, x, y);

    init(collection, params, x, y, bucketSort, dimensionX, dimensionY, metric);
  }


  public void setStreamContext(StreamContext context) {
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    return new ArrayList();
  }

  public void open() throws IOException {
    if (cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      cloudSolrClient = new Builder(hosts, Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build();
    }
    FieldComparator[] adjustedSorts = adjustSorts(x, y, bucketSort);
    this.resortNeeded = resortNeeded(adjustedSorts);

    String json = getJsonFacetString(x, y, metric, adjustedSorts, dimensionX, dimensionY);
    assert expectedJson(json);

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(params);
    paramsLoc.set("json.facet", json);
    paramsLoc.set("rows", "0");

    QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
    try {
      NamedList response = cloudSolrClient.request(request, collection);
      getTuples(response, x, y, metric);

      if (resortNeeded) {
        Collections.sort(tuples, getStreamSort());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Tuple read() throws IOException {
    this.index = 0;
    if (index < tuples.size() && index < dimensionX * dimensionY) {
      Tuple tuple = tuples.get(index);
      ++index;
      return tuple;
    } else {
      Map fields = new HashMap();
      fields.put("rows", tuples.size());

      fields.put("EOF", true);
      Tuple tuple = new Tuple(fields);
      return tuple;
    }

  }

  public void close() throws IOException {
    if (cache == null) {
      if (cloudSolrClient != null) {
        cloudSolrClient.close();
      }
    }
  }

  private boolean expectedJson(String json) {
    if (this.refine) {
      if (!json.contains("\"refine\":true")) {
        return false;
      } else {
        if (!json.contains("\"limit\":" + (this.dimensionX)) || !json.contains("\"limit\":" + (this.dimensionY))) {
          return false;
        }
      }
      if (!json.contains("\"" + x.toString() + "\":") || !json.contains("\"" + y.toString() + "\":")) {
        return false;
      }
      String function = metric.getFunctionName();
      if (!function.equals("count")) {
        if (!json.contains(metric.getIdentifier())) {
          return false;
        }
      }
    }
    return true;
  }

  private String getJsonFacetString(Bucket x, Bucket y, Metric metric, FieldComparator[] adjustedSorts, int dimensionX, int dimensionY) {
    StringBuilder buf = new StringBuilder();
    appendJson(buf, x, y, metric, adjustedSorts, dimensionX, dimensionY);

    return "{" + buf.toString() + "}";

  }

  private FieldComparator[] adjustSorts(Bucket x, Bucket y, FieldComparator bucketSort) throws IOException {
    FieldComparator[] adjustSorts = new FieldComparator[2];
    if (bucketSort.getLeftFieldName().contains("(")) {
      for (int i = 0; i < adjustSorts.length; i++) {
        adjustSorts[i] = bucketSort;
      }
    } else {
      adjustSorts[0] = new FieldComparator(x.toString(), bucketSort.getOrder());
      adjustSorts[1] = new FieldComparator(y.toString(), bucketSort.getOrder());

    }
    return adjustSorts;
  }

  private boolean resortNeeded(FieldComparator[] fieldComparators) {
    for (FieldComparator fieldComparator : fieldComparators) {
      if (fieldComparator.getLeftFieldName().contains("(")) {
        return true;
      }
    }
    return false;
  }

  private void appendJson(StringBuilder buf, Bucket x, Bucket y, Metric metric, FieldComparator[] adjustedSorts, int dimensionX, int dimensionY) {

    buf.append('"');
    buf.append(x.toString());
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"terms\"");
    buf.append(",\"field\":\"" + x.toString() + "\"");
    buf.append(",\"limit\":" + dimensionX);
    String fsort = getFacetSort(adjustedSorts[0].getLeftFieldName(), metric);
    buf.append(",\"sort\":{\"" + fsort + "\":\"" + adjustedSorts[0].getOrder() + "\"}");
    buf.append(",\"facet\":{");

    String identifier = metric.getIdentifier();
    if (!identifier.startsWith("count(")) {
      buf.append("\"agg" + "\":\"" + identifier + "\"");
    }

    buf.append('"');
    buf.append(y.toString());
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"terms\"");
    buf.append(",\"field\":\"" + y.toString() + "\"");
    buf.append(",\"limit\":" + dimensionY);
    fsort = getFacetSort(adjustedSorts[1].getLeftFieldName(), metric);
    buf.append(",\"sort\":{\"" + fsort + "\":\"" + adjustedSorts[1].getOrder() + "\"}");
    buf.append(",\"facet\":{");
    if (!identifier.startsWith("count(")) {
      buf.append("\"agg" + "\":\"" + identifier + "\"");
    }
    buf.append("}}");
  }

  private String getFacetSort(String id, Metric metric) {
    int index = 0;
    int metricCount = 0;
    if (metric.getIdentifier().startsWith("count(")) {
      if (id.startsWith("count(")) {
        return "count";
      }
      ++index;
    } else {
      if (id.equals(metric.getIdentifier())) {
        return "agg:" + metricCount;
      }
      ++index;
      ++metricCount;
    }
    return "index";
  }

  private void getTuples(NamedList response, Bucket x, Bucket y, Metric metric) {
    Tuple tuple = new Tuple(new HashMap());
    NamedList facets = (NamedList) response.get("facets");
    fillTuples(0, tuples, tuple, facets, x, y, metric);
  }

  private void fillTuples(int level, List<Tuple> tuples, Tuple currentTuple, NamedList facets, Bucket x, Bucket y, Metric metric) {
    String bucketXName = x.toString();
    NamedList nlX = (NamedList) facets.get(bucketXName);

    String bucketYName = y.toString();
    NamedList nlY = (NamedList) facets.get(bucketYName);

    if (nlX == null || nlY == null) {
      return;
    }
    List allXBuckets = (List) nlX.get("x");
    for (int b = 0; b < allXBuckets.size(); b++) {
      NamedList bucket = (NamedList) allXBuckets.get(b);
      Object val = bucket.get("val");
      if (val instanceof Integer) {
        val = ((Integer) val).longValue();
      }
      Tuple t = currentTuple.clone();
      t.put(bucketXName, val);

      List allYBuckets = (List) nlY.get("y");
      for (int d = 0; d < allYBuckets.size(); d++) {
        NamedList bucketY = (NamedList) allYBuckets.get(d);
        Object valY = bucketY.get("val");
        if (valY instanceof Integer) {
          valY = ((Integer) valY).longValue();
        }
        t = currentTuple.clone();
        t.put(bucketYName, valY);
      }
      int nextLevel = level + 1;
      if (nextLevel < 2) {
        fillTuples(nextLevel, tuples, t.clone(), bucket, x, y, metric);
      } else {
        int m = 0;
        String identifier = metric.getIdentifier();
        if (!identifier.startsWith("count(")) {
          Number d = (Number) bucket.get("agg:" + m);
          if (metric.outputLong) {
            if (d instanceof Long || d instanceof Integer) {
              t.put(identifier, d.longValue());
            } else {
              t.put(identifier, Math.round(d.doubleValue()));
            }
          } else {
            t.put(identifier, d.doubleValue());
          }
          ++m;
        } else {
          long l = ((Number) bucket.get("count")).longValue();
          t.put("count(*)", l);
        }
      }
      tuples.add(t);
    }


  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return bucketSort;
  }

  public String getCollection() {
    return collection;
  }

  public int getDimensionX() {
    return dimensionX;
  }

  public int getDimensionY() {
    return dimensionY;
  }

  public Bucket getX() {
    return x;
  }

  public Bucket getY() {
    return y;
  }
}