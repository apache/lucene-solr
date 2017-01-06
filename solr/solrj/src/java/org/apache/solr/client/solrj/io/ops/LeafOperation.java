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
package org.apache.solr.client.solrj.io.ops;

import java.io.IOException;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class LeafOperation implements BooleanOperation {

  private static final long serialVersionUID = 1;
  private UUID operationNodeId = UUID.randomUUID();

  protected String field;
  protected Double val;
  protected Tuple tuple;

  public void operate(Tuple tuple) {
    this.tuple = tuple;
  }

  public LeafOperation(String field, double val) {
    this.field = field;
    this.val = val;
  }

  public LeafOperation(StreamExpression expression, StreamFactory factory) throws IOException {
    this.field = factory.getValueOperand(expression, 0);
    this.val = Double.parseDouble(factory.getValueOperand(expression, 1));
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(operationNodeId.toString())
        .withExpressionType(ExpressionType.OPERATION)
        .withFunctionName(factory.getFunctionName(getClass()))
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }

  protected String quote(String s) {
    if(s.contains("(")) {
      return "'"+s+"'";
    }

    return s;
  }
}
