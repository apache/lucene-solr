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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.SourceEvaluator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.lucene.analysis.*;
import org.apache.solr.core.SolrCore;

public class AnalyzeEvaluator extends SourceEvaluator {
  private static final long serialVersionUID = 1L;

  private String fieldName;
  private String analyzerField;
  private Analyzer analyzer;

  public AnalyzeEvaluator(String _fieldName, String _analyzerField) {
    init(_fieldName, _analyzerField);
  }

  public AnalyzeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    String _fieldName = factory.getValueOperand(expression, 0);
    String _analyzerField = factory.getValueOperand(expression, 1);
    init(_fieldName, _analyzerField);
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
    Object solrCoreObj = context.get("solr-core");
    if (solrCoreObj == null || !(solrCoreObj instanceof SolrCore) ) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "StreamContext must have SolrCore in solr-core key");
    }

    analyzer = ((SolrCore) solrCoreObj).getLatestSchema().getFieldType(analyzerField).getIndexAnalyzer();
  }

  private void init(String fieldName, String analyzerField) {
    this.fieldName = fieldName;
    if(analyzerField == null) {
      this.analyzerField = fieldName;
    } else {
      this.analyzerField = analyzerField;
    }
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {
    String value = null;
    Object obj = tuple.get(fieldName);

    if(obj == null) {
      value = fieldName;
    } else {
      value = obj.toString();
    }

    List<String> tokens = new ArrayList<>();

    try(TokenStream tokenStream = analyzer.tokenStream(analyzerField, value)) {
      CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        tokens.add(termAtt.toString());
      }
      tokenStream.end();
    }
    return tokens;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpressionValue(fieldName);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
        .withExpressionType(ExpressionType.EVALUATOR)
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }

}
