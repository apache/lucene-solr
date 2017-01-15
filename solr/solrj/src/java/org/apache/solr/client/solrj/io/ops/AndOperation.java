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

public class AndOperation implements BooleanOperation {

  private static final long serialVersionUID = 1;
  private UUID operationNodeId = UUID.randomUUID();

  protected BooleanOperation leftOperand;
  protected BooleanOperation rightOperand;

  public void operate(Tuple tuple) {
    leftOperand.operate(tuple);
    rightOperand.operate(tuple);
  }

  public AndOperation(BooleanOperation leftOperand, BooleanOperation rightOperand) {
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  public AndOperation(StreamExpression expression, StreamFactory factory) throws IOException {
      List<StreamExpression> operationExpressions = factory.getExpressionOperandsRepresentingTypes(expression, BooleanOperation.class);
      if(operationExpressions != null && operationExpressions.size() == 2) {
        StreamExpression left = operationExpressions.get(0);
        StreamOperation leftOp = factory.constructOperation(left);
        if(leftOp instanceof BooleanOperation) {
          leftOperand = (BooleanOperation) leftOp;
        } else {
          throw new IOException("The And/Or Operation requires a BooleanOperation.");
        }

        StreamExpression right = operationExpressions.get(1);
        StreamOperation rightOp = factory.constructOperation(right);
        if(rightOp instanceof BooleanOperation) {
          rightOperand = (BooleanOperation) rightOp;
        } else {
          throw new IOException("The And/Or Operation requires a BooleanOperation.");
        }
      } else {
        throw new IOException("The And/Or Operation requires a BooleanOperations.");
      }
  }

  public boolean evaluate() {
    return leftOperand.evaluate() && rightOperand.evaluate();
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    if(leftOperand instanceof Expressible) {
      expression.addParameter(leftOperand.toExpression(factory));
    } else {
      throw new IOException("This left operand of the AndOperation contains a non-expressible operation - it cannot be converted to an expression");
    }

    if(rightOperand instanceof Expressible) {
      expression.addParameter(rightOperand.toExpression(factory));
    } else {
      throw new IOException("This the right operand of the AndOperation contains a non-expressible operation - it cannot be converted to an expression");
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
