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
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.ModifiableSolrParams;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.FL;
import static org.apache.solr.common.params.CommonParams.Q;
import static org.apache.solr.common.params.CommonParams.SORT;


public class DrillStream extends CloudSolrStream implements Expressible {

  private TupleStream tupleStream;
  private transient StreamFactory streamFactory;
  private String sort;
  private String fl;
  private String q;

  public DrillStream(String zkHost,
                    String collection,
                    TupleStream tupleStream,
                    StreamComparator comp,
                    String sortParam,
                    String flParam,
                    String qParam) throws IOException {
    init(zkHost,
        collection,
        tupleStream,
        comp,
        sortParam,
        flParam,
        qParam);
  }

  public DrillStream(String zkHost,
                    String collection,
                    String expressionString,
                    StreamComparator comp,
                    String sortParam,
                    String flParam,
                    String qParam) throws IOException {
    TupleStream tStream = this.streamFactory.constructStream(expressionString);
    init(zkHost,
        collection,
        tStream,
        comp,
        sortParam,
        flParam,
        qParam);
  }

  public void setStreamFactory(StreamFactory streamFactory) {
    this.streamFactory = streamFactory;
  }

  public DrillStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, SORT);
    StreamExpressionNamedParameter qExpression = factory.getNamedOperand(expression, Q);
    StreamExpressionNamedParameter flExpression = factory.getNamedOperand(expression, FL);


    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }


    // Stream
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    String sortParam = null;

    // Sort
    if(null == sortExpression || !(sortExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'sort' parameter but didn't find one",expression));
    } else {
      sortParam = ((StreamExpressionValue)sortExpression.getParameter()).getValue();
    }

    String flParam = null;

    // fl
    if(null == flExpression || !(flExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'fl' parameter but didn't find one",expression));
    } else {
      flParam = ((StreamExpressionValue)flExpression.getParameter()).getValue();
    }

    String qParam = null;

    // q
    if(null == qExpression || !(qExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'q' parameter but didn't find one",expression));
    } else {
      qParam = ((StreamExpressionValue)qExpression.getParameter()).getValue();
    }

    // zkHost, optional - if not provided then will look into factory list to get
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

    // We've got all the required items
    StreamFactory localFactory = (StreamFactory)factory.clone();
    localFactory.withDefaultSort(sortParam);
    TupleStream stream = localFactory.constructStream(streamExpressions.get(0));
    StreamComparator comp = localFactory.constructComparator(((StreamExpressionValue)sortExpression.getParameter()).getValue(), FieldComparator.class);
    streamFactory = factory;
    init(zkHost,collectionName,stream, comp, sortParam, flParam, qParam);
  }

  private void init(String zkHost,
                    String collection,
                    TupleStream tupleStream,
                    StreamComparator comp,
                    String sortParam,
                    String flParam,
                    String qParam) throws IOException{
    this.zkHost = zkHost;
    this.collection = collection;
    this.comp = comp;
    this.tupleStream = tupleStream;
    this.fl = flParam;
    this.q = qParam;
    this.sort = sortParam;

    // requires Expressible stream and comparator
    if(! (tupleStream instanceof Expressible)){
      throw new IOException("Unable to create DrillStream with a non-expressible TupleStream.");
    }
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);


    if(includeStreams){
      if(tupleStream instanceof Expressible){
        expression.addParameter(((Expressible)tupleStream).toExpression(factory));
      }
      else{
        throw new IOException("This DrillStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }

    // sort
    expression.addParameter(new StreamExpressionNamedParameter(SORT,comp.toExpression(factory)));

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());


    return explanation;
  }

  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(tupleStream);
    return l;
  }

  public Tuple read() throws IOException {
    Tuple tuple = _read();

    if(tuple.EOF) {
      return Tuple.EOF();
    }

    return tuple;
  }

  public void setStreamContext(StreamContext streamContext) {
    this.streamContext = streamContext;
    if(streamFactory == null) {
      this.streamFactory = streamContext.getStreamFactory();
    }
    this.tupleStream.setStreamContext(streamContext);
  }

  protected void constructStreams() throws IOException {
    try {
      Object pushStream = ((Expressible) tupleStream).toExpression(streamFactory);
      final ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
      paramsLoc.set(DISTRIB,"false"); // We are the aggregator.
      paramsLoc.set("expr", pushStream.toString());
      paramsLoc.set("qt","/export");
      paramsLoc.set("fl", fl);
      paramsLoc.set("sort", sort);
      paramsLoc.set("q", q);
      getReplicas(this.zkHost, this.collection, this.streamContext, paramsLoc).forEach(r -> {
        SolrStream solrStream = new SolrStream(r.getBaseUrl(), paramsLoc, r.getCoreName());
        solrStream.setStreamContext(streamContext);
        solrStreams.add(solrStream);
      });
    } catch (Exception e) {
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      if (rootCause instanceof IOException) {
        throw (IOException)rootCause;
      } else {
        throw new IOException(e);
      }
    }
  }
}