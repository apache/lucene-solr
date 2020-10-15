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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import static org.apache.solr.common.params.CommonParams.SORT;

public class GroupOperation implements ReduceOperation {

  private static final long serialVersionUID = 1L;
  private UUID operationNodeId = UUID.randomUUID();
  
  private PriorityQueue<Tuple> priorityQueue;
  @SuppressWarnings({"rawtypes"})
  private Comparator comp;
  private StreamComparator streamComparator;
  private int size;

  public GroupOperation(StreamExpression expression, StreamFactory factory) throws IOException {

    StreamExpressionNamedParameter nParam = factory.getNamedOperand(expression, "n");
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, SORT);

    StreamComparator streamComparator = factory.constructComparator(((StreamExpressionValue) sortExpression.getParameter()).getValue(), FieldComparator.class);
    String nStr = ((StreamExpressionValue)nParam.getParameter()).getValue();
    int nInt = 0;

    try{
      nInt = Integer.parseInt(nStr);
      if(nInt <= 0){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topN '%s' must be greater than 0.",expression, nStr));
      }
    } catch(NumberFormatException e) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topN '%s' is not a valid integer.",expression, nStr));
    }

    init(streamComparator, nInt);
  }

  public GroupOperation(StreamComparator streamComparator, int size) {
    init(streamComparator, size);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void init(StreamComparator streamComparator, int size) {
    this.size = size;
    this.streamComparator = streamComparator;
    this.comp = new ReverseComp(streamComparator);
    this.priorityQueue = new PriorityQueue(size, this.comp);
  }

  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    // n
    expression.addParameter(new StreamExpressionNamedParameter("n", Integer.toString(size)));

    // sort
    expression.addParameter(new StreamExpressionNamedParameter(SORT, streamComparator.toExpression(factory)));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(operationNodeId.toString())
      .withExpressionType(ExpressionType.OPERATION)
      .withFunctionName(factory.getFunctionName(getClass()))
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString())
      .withHelpers(new Explanation[]{
          streamComparator.toExplanation(factory)
      });
  }

  @SuppressWarnings({"unchecked"})
  public Tuple reduce() {
    @SuppressWarnings({"rawtypes"})
    LinkedList ll = new LinkedList();
    while(priorityQueue.size() > 0) {
      ll.addFirst(priorityQueue.poll().getFields());
      //This will clear priority queue and so it will be ready for the next group.
    }

    @SuppressWarnings({"rawtypes"})
    List<Map> list = new ArrayList<>(ll);
    @SuppressWarnings({"rawtypes"})
    Map groupHead = list.get(0);
    Tuple tuple = new Tuple(groupHead);
    tuple.put("group", list);
    return tuple;
  }

  public void operate(Tuple tuple) {
    if(priorityQueue.size() >= size) {
      Tuple peek = priorityQueue.peek();
      if(streamComparator.compare(tuple, peek) < 0) {
        priorityQueue.poll();
        priorityQueue.add(tuple);
      }
    } else {
      priorityQueue.add(tuple);
    }
  }

  static class ReverseComp implements Comparator<Tuple>, Serializable {
    private StreamComparator comp;

    public ReverseComp(StreamComparator comp) {
      this.comp = comp;
    }

    public int compare(Tuple t1, Tuple t2) {
      // Couldn't this be comp.compare(t2,t1) ?
      return comp.compare(t1, t2)*(-1);
    }
  }
}
