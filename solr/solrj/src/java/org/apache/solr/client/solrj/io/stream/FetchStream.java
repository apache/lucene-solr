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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.ModifiableSolrParams;

import static org.apache.solr.common.params.CommonParams.SORT;
import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

/**
 *  Iterates over a stream and fetches additional fields from a specified collection.
 *  Fetches are done in batches.
 *
 *  Syntax:
 *
 *  fetch(collection, stream, on="a=b", fl="c,d,e", batchSize="50")
 *
 * @since 6.3.0
 **/

public class FetchStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  private TupleStream stream;
  private StreamContext streamContext;
  private Iterator<Tuple> tuples;

  private String leftKey;
  private String rightKey;
  private String fieldList;
  private String[] fields;
  private String collection;
  private int batchSize;
  private boolean appendVersion = true;
  private boolean appendKey = true;

  public FetchStream(String zkHost, String collection, TupleStream tupleStream, String on, String fieldList, int batchSize) throws IOException {
    init(zkHost, collection, tupleStream, on, fieldList, batchSize);
  }

  public FetchStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter onParam = factory.getNamedOperand(expression, "on");
    StreamExpressionNamedParameter flParam = factory.getNamedOperand(expression, "fl");
    StreamExpressionNamedParameter batchSizeParam = factory.getNamedOperand(expression, "batchSize");
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    String on = null;
    String fl = null;
    int batchSize = 50;

    if(onParam == null)  {
      throw new IOException("on parameter cannot be null for the fetch expression");
    } else {
      on = ((StreamExpressionValue)onParam.getParameter()).getValue();
    }

    if(flParam == null)  {
      throw new IOException("fl parameter cannot be null for the fetch expression");
    } else {
      fl = ((StreamExpressionValue)flParam.getParameter()).getValue();
    }

    if(batchSizeParam != null)  {
      batchSize = Integer.parseInt(((StreamExpressionValue)batchSizeParam.getParameter()).getValue());
    }

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    TupleStream stream = factory.constructStream(streamExpressions.get(0));

    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    }
    else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }

    init(zkHost, collectionName, stream, on, fl, batchSize);
  }

  private void init(String zkHost, String collection, TupleStream tupleStream, String on, String fieldList, int batchSize) throws IOException{
    this.zkHost = zkHost;
    this.collection = collection;
    this.stream = tupleStream;
    this.batchSize = batchSize;
    this.fields = fieldList.split(",");
    this.fieldList = fieldList;

    if(on.indexOf("=") > -1) {
      String[] leftright = on.split("=");
      leftKey = leftright[0].trim();
      rightKey = leftright[1].trim();
    } else {
      leftKey = rightKey = on;
    }

    for(int i=0; i<fields.length; i++) {
      fields[i] = fields[i].trim();
      if(fields[i].equals(VERSION_FIELD)) {
        appendVersion = false;
      }

      if(fields[i].equals(rightKey)) {
        appendKey = false;
      }
    }
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(collection);
    expression.addParameter(new StreamExpressionNamedParameter("on", leftKey+"="+rightKey));
    expression.addParameter(new StreamExpressionNamedParameter("fl", fieldList));
    expression.addParameter(new StreamExpressionNamedParameter("batchSize", Integer.toString(batchSize)));

    // stream
    if(includeStreams) {
      if (stream instanceof Expressible) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      } else {
        throw new IOException("The FetchStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
            stream.toExplanation(factory)
        })
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory, false).toString());
  }

  public void setStreamContext(StreamContext streamContext) {
    this.streamContext = streamContext;
    this.stream.setStreamContext(streamContext);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<>();
    l.add(stream);
    return l;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void open() throws IOException {
    tuples = new ArrayList().iterator();
    stream.open();
  }

  private void fetchBatch() throws IOException {
    Tuple EOFTuple = null;
    List<Tuple> batch = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++) {
      Tuple tuple = stream.read();
      if(tuple.EOF) {
        EOFTuple = tuple;
        break;
      } else {
        batch.add(tuple);
      }
    }

    if(batch.size() > 0) {
      StringBuilder buf = new StringBuilder(batch.size() * 10 + 20);
      buf.append("{! df=").append(rightKey).append(" q.op=OR cache=false }");//disable queryCache
      for (Tuple tuple : batch) {
        String key = tuple.getString(leftKey);
        buf.append(' ').append(ClientUtils.escapeQueryChars(key));
      }

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", buf.toString());
      params.add("fl", fieldList+appendFields());
      params.add("rows", Integer.toString(batchSize));
      params.add(SORT, "_version_ desc");

      CloudSolrStream cloudSolrStream = new CloudSolrStream(zkHost, collection, params);
      StreamContext newContext = new StreamContext();
      newContext.setSolrClientCache(streamContext.getSolrClientCache());
      newContext.setObjectCache(streamContext.getObjectCache());
      cloudSolrStream.setStreamContext(newContext);
      Map<String, Tuple> fetched = new HashMap<>();
      try {
        cloudSolrStream.open();
        while (true) {
          Tuple t = cloudSolrStream.read();
          if (t.EOF) {
            break;
          } else {
            String rightValue = t.getString(rightKey);
            fetched.put(rightValue, t);
          }
        }
      } finally {
        cloudSolrStream.close();
      }

      //Iterate the batch and add the fetched fields to the Tuples
      for (Tuple batchTuple : batch) {
        Tuple fetchedTuple = fetched.get(batchTuple.getString(leftKey));
        if(fetchedTuple !=null) {
          for (String field : fields) {
            Object value = fetchedTuple.get(field);
            if(value != null) {
              batchTuple.put(field, value);
            }
          }
        }
      }
    }

    if(EOFTuple != null) {
      batch.add(EOFTuple);
    }

    this.tuples = batch.iterator();
  }

  public void close() throws IOException {
    stream.close();
  }

  public Tuple read() throws IOException {
    if(!tuples.hasNext()) {
      fetchBatch();
    }

    return tuples.next();
  }

  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }

  private String appendFields() {
    StringBuffer buf = new StringBuffer();
    if(appendKey) {
      buf.append(",");
      buf.append(rightKey);
    }

    if(appendVersion) {
      buf.append(",_version_");
    }
    return buf.toString();
  }
}
