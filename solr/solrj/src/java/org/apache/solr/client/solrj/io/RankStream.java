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

package org.apache.solr.client.solrj.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Comparator;


/**
*  Iterates over a TupleStream and Ranks the topN tuples based on a Comparator.
**/

public class RankStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private PriorityQueue<Tuple> top;
  private Comparator<Tuple> comp;
  private boolean finished = false;
  private LinkedList<Tuple> topList;
  private int size;

  public RankStream(TupleStream tupleStream, int size, Comparator<Tuple> comp) {
    this.tupleStream = tupleStream;
    this.top = new PriorityQueue(size, new ReverseComp(comp));
    this.comp = comp;
    this.topList = new LinkedList();
    this.size = size;
  }

  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {
    if(!finished) {
      while(true) {
        Tuple tuple = tupleStream.read();
        if(tuple.EOF) {
          finished = true;
          int s = top.size();
          for(int i=0; i<s; i++) {
            Tuple t = top.poll();
            topList.addFirst(t);
          }
          topList.addLast(tuple);
          break;
        } else {
          Tuple peek = top.peek();
          if(top.size() >= size) {
            if(comp.compare(tuple, peek) < 0) {
              top.poll();
              top.add(tuple);
            }
          } else {
            top.add(tuple);
          }
        }
      }
    }

    return topList.pollFirst();
  }

  public int getCost() {
    return 0;
  }

  class ReverseComp implements Comparator<Tuple>, Serializable {

    private Comparator<Tuple> comp;

    public ReverseComp(Comparator<Tuple> comp) {
      this.comp = comp;
    }

    public int compare(Tuple t1, Tuple t2) {
      return comp.compare(t1, t2)*(-1);
    }
  }
}