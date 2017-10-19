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
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Locale;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.lucene.analysis.*;

/**
 *  The classify expression retrieves a model trained by the train expression and uses it to classify documents from a stream
 *  Syntax:
 *  classif(model(...), anyStream(...), field="body")
 * @since 6.3.0
 **/

public class ClassifyStream extends TupleStream implements Expressible {
  private TupleStream docStream;
  private TupleStream modelStream;

  private String field;
  private String analyzerField;
  private Tuple  modelTuple;

  Analyzer analyzer;
  private Map<CharSequence, Integer> termToIndex;
  private List<Double> idfs;
  private List<Double> modelWeights;

  public ClassifyStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    if (streamExpressions.size() != 2) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two stream but found %d",expression, streamExpressions.size()));
    }

    modelStream = factory.constructStream(streamExpressions.get(0));
    docStream = factory.constructStream(streamExpressions.get(1));

    StreamExpressionNamedParameter fieldParameter = factory.getNamedOperand(expression, "field");
    if (fieldParameter == null) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - field parameter must be specified",expression, streamExpressions.size()));
    }
    analyzerField = field = fieldParameter.getParameter().toString();

    StreamExpressionNamedParameter analyzerFieldParameter = factory.getNamedOperand(expression, "analyzerField");
    if (analyzerFieldParameter != null) {
      analyzerField = analyzerFieldParameter.getParameter().toString();
    }
  }

  @Override
  public void setStreamContext(StreamContext context) {
    Object solrCoreObj = context.get("solr-core");
    if (solrCoreObj == null || !(solrCoreObj instanceof SolrCore) ) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "StreamContext must have SolrCore in solr-core key");
    }
    analyzer = ((SolrCore) solrCoreObj).getLatestSchema().getFieldType(analyzerField).getIndexAnalyzer();

    this.docStream.setStreamContext(context);
    this.modelStream.setStreamContext(context);
  }

  @Override
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(docStream);
    l.add(modelStream);
    return l;
  }

  @Override
  public void open() throws IOException {
    this.docStream.open();
    this.modelStream.open();
  }

  @Override
  public void close() throws IOException {
    this.docStream.close();
    this.modelStream.close();
  }

  @Override
  public Tuple read() throws IOException {
    if (modelTuple == null) {

      modelTuple = modelStream.read();
      if (modelTuple == null || modelTuple.EOF) {
        throw new IOException("Model tuple not found for classify stream!");
      }

      termToIndex = new HashMap<>();

      List<String> terms = modelTuple.getStrings("terms_ss");

      for (int i = 0; i < terms.size(); i++) {
        termToIndex.put(terms.get(i), i);
      }

      idfs = modelTuple.getDoubles("idfs_ds");
      modelWeights = modelTuple.getDoubles("weights_ds");
    }

    Tuple docTuple = docStream.read();
    if (docTuple.EOF) return docTuple;

    String text = docTuple.getString(field);

    double tfs[] = new double[termToIndex.size()];

    TokenStream tokenStream = analyzer.tokenStream(analyzerField, text);
    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.reset();

    int termCount = 0;
    while (tokenStream.incrementToken()) {
      termCount++;
      if (termToIndex.containsKey(termAtt.toString())) {
        tfs[termToIndex.get(termAtt.toString())]++;
      }
    }

    tokenStream.end();
    tokenStream.close();

    List<Double> tfidfs = new ArrayList<>(termToIndex.size());
    tfidfs.add(1.0);
    for (int i = 0; i < tfs.length; i++) {
      if (tfs[i] != 0) {
        tfs[i] = 1 + Math.log(tfs[i]);
      }
      tfidfs.add(this.idfs.get(i) * tfs[i]);
    }

    double total = 0.0;
    for (int i = 0; i < tfidfs.size(); i++) {
      total += tfidfs.get(i) * modelWeights.get(i);
    }

    double score = total * ((float) (1.0 / Math.sqrt(termCount)));
    double positiveProb = sigmoid(total);

    docTuple.put("probability_d", positiveProb);
    docTuple.put("score_d",  score);

    return docTuple;
  }

  private double sigmoid(double in) {
    double d = 1.0 / (1+Math.exp(-in));
    return d;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    if (includeStreams) {
      if (docStream instanceof Expressible && modelStream instanceof Expressible) {
        expression.addParameter(((Expressible)modelStream).toExpression(factory));
        expression.addParameter(((Expressible)docStream).toExpression(factory));
      } else {
        throw new IOException("This ClassifyStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }

    expression.addParameter(new StreamExpressionNamedParameter("field", field));
    expression.addParameter(new StreamExpressionNamedParameter("analyzerField", analyzerField));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(Explanation.ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());

    explanation.addChild(docStream.toExplanation(factory));
    explanation.addChild(modelStream.toExplanation(factory));

    return explanation;
  }
}
