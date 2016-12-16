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
import java.util.List;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


public class NotOperation implements BooleanOperation {

  private static final long serialVersionUID = 1;
  private UUID operationNodeId = UUID.randomUUID();

  protected BooleanOperation operand;

  public void operate(Tuple tuple) {
    operand.operate(tuple);
  }

  public NotOperation(BooleanOperation operand) {
    this.operand = operand;
  }

  public NotOperation(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> operationExpressions = factory.getExpressionOperandsRepresentingTypes(expression, BooleanOperation.class);
    if(operationExpressions != null && operationExpressions.size() == 1) {
      StreamExpression op = operationExpressions.get(0);
      StreamOperation streamOp = factory.constructOperation(op);
      if(streamOp instanceof BooleanOperation) {
        operand = (BooleanOperation) streamOp;
      } else {
        throw new IOException("The NotOperation requires a BooleanOperation.");
      }

    } else {
      throw new IOException("The NotOperation requires a BooleanOperations.");
    }
  }

  public boolean evaluate() {
    return !operand.evaluate();
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    if(operand instanceof Expressible) {
      expression.addParameter(operand.toExpression(factory));
    } else {
      throw new IOException("The operand of the NotOperation contains a non-expressible operation - it cannot be converted to an expression");
    }
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(operationNodeId.toString())
        .withExpressionType(ExpressionType.OPERATION)
        .withFunctionName(factory.getFunctionName(getClass()))
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }

}
