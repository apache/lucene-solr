package org.apache.solr.client.solrj.io.ops;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;

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

public class GroupOperation implements ReduceOperation {

  private PriorityQueue<Tuple> priorityQueue;
  private Comparator comp;
  private StreamComparator streamComparator;
  private int size;

  public GroupOperation(StreamExpression expression, StreamFactory factory) throws IOException {

    StreamExpressionNamedParameter nParam = factory.getNamedOperand(expression, "n");
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, "sort");

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
    expression.addParameter(new StreamExpressionNamedParameter("sort", streamComparator.toExpression(factory)));
    return expression;
  }

  public Tuple reduce() {
    Map map = new HashMap();
    List<Map> list = new ArrayList();
    LinkedList ll = new LinkedList();
    while(priorityQueue.size() > 0) {
      ll.addFirst(priorityQueue.poll().getMap());
      //This will clear priority queue and so it will be ready for the next group.
    }

    list.addAll(ll);
    Map groupHead = list.get(0);
    map.putAll(groupHead);
    map.put("group", list);
    return new Tuple(map);
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

  class ReverseComp implements Comparator<Tuple>, Serializable {
    private StreamComparator comp;

    public ReverseComp(StreamComparator comp) {
      this.comp = comp;
    }

    public int compare(Tuple t1, Tuple t2) {
      return comp.compare(t1, t2)*(-1);
    }
  }
}
