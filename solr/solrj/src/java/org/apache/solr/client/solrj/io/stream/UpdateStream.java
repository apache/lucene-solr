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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sends tuples emitted by a wrapped {@link TupleStream} as updates to a SolrCloud collection.
 * @since 6.0.0
 */
public class UpdateStream extends TupleStream implements Expressible {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String BATCH_INDEXED_FIELD_NAME = "batchIndexed"; // field name in summary tuple for #docs updated in batch
  private String collection;
  private String zkHost;
  private int updateBatchSize;
  /**
   * Indicates if the {@link CommonParams#VERSION_FIELD} should be removed from tuples when converting 
   * to Solr Documents.  
   * May be set per expression using the <code>"pruneVersionField"</code> named operand, 
   * defaults to the value returned by {@link #defaultPruneVersionField()} 
   */
  private boolean pruneVersionField;
  private int batchNumber;
  private long totalDocsIndex;
  private PushBackStream tupleSource;
  private transient SolrClientCache cache;
  private transient CloudSolrClient cloudSolrClient;
  private List<SolrInputDocument> documentBatch = new ArrayList<>();
  private String coreName;

  public UpdateStream(StreamExpression expression, StreamFactory factory) throws IOException {
    String collectionName = factory.getValueOperand(expression, 0);
    verifyCollectionName(collectionName, expression);
    
    String zkHost = findZkHost(factory, collectionName, expression);
    verifyZkHost(zkHost, collectionName, expression);
    
    int updateBatchSize = extractBatchSize(expression, factory);
    pruneVersionField = factory.getBooleanOperand(expression, "pruneVersionField", defaultPruneVersionField());

    //Extract underlying TupleStream.
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    if (1 != streamExpressions.size()) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    StreamExpression sourceStreamExpression = streamExpressions.get(0);
    init(collectionName, factory.constructStream(sourceStreamExpression), zkHost, updateBatchSize);
  }
  
  public UpdateStream(String collectionName, TupleStream tupleSource, String zkHost, int updateBatchSize) throws IOException {
    if (updateBatchSize <= 0) {
      throw new IOException(String.format(Locale.ROOT,"batchSize '%d' must be greater than 0.", updateBatchSize));
    }
    pruneVersionField = defaultPruneVersionField();
    init(collectionName, tupleSource, zkHost, updateBatchSize);
  }

  private void init(String collectionName, TupleStream tupleSource, String zkHost, int updateBatchSize) {
    this.collection = collectionName;
    this.zkHost = zkHost;
    this.updateBatchSize = updateBatchSize;
    this.tupleSource = new PushBackStream(tupleSource);
  }
  
  /** The name of the collection being updated */
  protected String getCollectionName() {
    return collection;
  }

  @Override
  public void open() throws IOException {
    setCloudSolrClient();
    tupleSource.open();
  }
  
  @Override
  public Tuple read() throws IOException {

    for (int i = 0; i < updateBatchSize; i++) {
      Tuple tuple = tupleSource.read();
      if (tuple.EOF) {
        if (documentBatch.isEmpty()) {
          return tuple;
        } else {
          tupleSource.pushBack(tuple);
          uploadBatchToCollection(documentBatch);
          int b = documentBatch.size();
          documentBatch.clear();
          return createBatchSummaryTuple(b);
        }
      }
      documentBatch.add(convertTupleToSolrDocument(tuple));
    }

    uploadBatchToCollection(documentBatch);
    int b = documentBatch.size();
    documentBatch.clear();
    return createBatchSummaryTuple(b);
  }
  
  @Override
  public void close() throws IOException {
    if(cache == null && cloudSolrClient != null) {
      cloudSolrClient.close();
    }
    tupleSource.close();
  }
  
  @Override
  public StreamComparator getStreamSort() {
    return tupleSource.getStreamSort();
  }
  
  @Override
  public List<TupleStream> children() {
    ArrayList<TupleStream> sourceList = new ArrayList<TupleStream>(1);
    sourceList.add(tupleSource);
    return sourceList;
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(collection);
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    expression.addParameter(new StreamExpressionNamedParameter("batchSize", Integer.toString(updateBatchSize)));
    
    if(includeStreams){
      if(tupleSource instanceof Expressible){
        expression.addParameter(((Expressible)tupleSource).toExpression(factory));
      } else {
        throw new IOException("This ParallelStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    // An update stream is backward wrt the order in the explanation. This stream is the "child"
    // while the collection we're updating is the parent.
    
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId() + "-datastore");
    
    explanation.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    explanation.setImplementingClass("Solr/Lucene");
    explanation.setExpressionType(ExpressionType.DATASTORE);
    explanation.setExpression("Update into " + collection);
    
    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId().toString());
    child.setFunctionName(String.format(Locale.ROOT, factory.getFunctionName(getClass())));
    child.setImplementingClass(getClass().getName());
    child.setExpressionType(ExpressionType.STREAM_DECORATOR);
    child.setExpression(toExpression(factory, false).toString());
    child.addChild(tupleSource.toExplanation(factory));
    
    explanation.addChild(child);
    
    return explanation;
  }
  
  @Override
  public void setStreamContext(StreamContext context) {
    this.cache = context.getSolrClientCache();
    this.coreName = (String)context.get("core");
    this.tupleSource.setStreamContext(context);
  }
  
  private void verifyCollectionName(String collectionName, StreamExpression expression) throws IOException {
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }
  }
  
  private String findZkHost(StreamFactory factory, String collectionName, StreamExpression expression) {
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    if(null == zkHostExpression){
      String zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        return factory.getDefaultZkHost();
      } else {
        return zkHost;
      }
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      return ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    
    return null;
  }
  
  private void verifyZkHost(String zkHost, String collectionName, StreamExpression expression) throws IOException {
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
  }
  
  private int extractBatchSize(StreamExpression expression, StreamFactory factory) throws IOException {
    StreamExpressionNamedParameter batchSizeParam = factory.getNamedOperand(expression, "batchSize");
    if(batchSizeParam == null) {
      // Sensible default batch size
      return 250;
    }
    String batchSizeStr = ((StreamExpressionValue)batchSizeParam.getParameter()).getValue();
    return parseBatchSize(batchSizeStr, expression);
  }
  
  private int parseBatchSize(String batchSizeStr, StreamExpression expression) throws IOException {
    try{
      int batchSize = Integer.parseInt(batchSizeStr);
      if(batchSize <= 0){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - batchSize '%d' must be greater than 0.",expression, batchSize));
      }
      return batchSize;
    }
    catch(NumberFormatException e){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - batchSize '%s' is not a valid integer.",expression, batchSizeStr));
    }    
  }

  /**
   * Used during initialization to specify the default value for the <code>"pruneVersionField"</code> option.
   * {@link UpdateStream} returns <code>true</code> for backcompat and to simplify slurping of data from one
   * collection to another.
   */
  protected boolean defaultPruneVersionField() {
    return true;
  }
  
  /** Only viable after calling {@link #open} */
  protected CloudSolrClient getCloudSolrClient() {
    assert null != this.cloudSolrClient;
    return this.cloudSolrClient;
  }
  
  private void setCloudSolrClient() {
    if(this.cache != null) {
      this.cloudSolrClient = this.cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      this.cloudSolrClient = new Builder(hosts, Optional.empty()).build();
      this.cloudSolrClient.connect();
    }
  }
  
  @SuppressWarnings({"unchecked"})
  private SolrInputDocument convertTupleToSolrDocument(Tuple tuple) {
    SolrInputDocument doc = new SolrInputDocument();
    for (Object field : tuple.getFields().keySet()) {

      if (! (field.equals(CommonParams.VERSION_FIELD) && pruneVersionField)) {
        Object value = tuple.get(field);
        if (value instanceof List) {
          addMultivaluedField(doc, (String)field, (List<Object>)value);
        } else {
          doc.addField((String)field, tuple.get(field));
        }
      }
    }
    log.debug("Tuple [{}] was converted into SolrInputDocument [{}].", tuple, doc);
    
    return doc;
  }
  
  private void addMultivaluedField(SolrInputDocument doc, String fieldName, List<Object> values) {
    for (Object value : values) {
      doc.addField(fieldName, value);
    }
  }
  
  /**
   * This method will be called on every batch of tuples comsumed, after converting each tuple 
   * in that batch to a Solr Input Document.
   */
  protected void uploadBatchToCollection(List<SolrInputDocument> documentBatch) throws IOException {
    if (documentBatch.size() == 0) {
      return;
    }
    
    try {
      cloudSolrClient.add(collection, documentBatch);
    } catch (SolrServerException | IOException e) {
      // TODO: it would be nice if there was an option to "skipFailedBatches"
      // TODO: and just record the batch failure info in the summary tuple for that batch and continue
      //
      // TODO: The summary batches (and/or stream error) should also pay attention to the error metadata
      // from the SolrServerException ... and ideally also any TolerantUpdateProcessor metadata

      log.warn("Unable to add documents to collection due to unexpected error.", e);
      String className = e.getClass().getName();
      String message = e.getMessage();
      throw new IOException(String.format(Locale.ROOT,"Unexpected error when adding documents to collection %s- %s:%s", collection, className, message));
    }
  }
  
  private Tuple createBatchSummaryTuple(int batchSize) {
    assert batchSize > 0;
    Tuple tuple = new Tuple();
    this.totalDocsIndex += batchSize;
    ++batchNumber;
    tuple.put(BATCH_INDEXED_FIELD_NAME, batchSize);
    tuple.put("totalIndexed", this.totalDocsIndex);
    tuple.put("batchNumber", batchNumber);
    if (coreName != null) {
      tuple.put("worker", coreName);
    }
    return tuple;
  }

}
