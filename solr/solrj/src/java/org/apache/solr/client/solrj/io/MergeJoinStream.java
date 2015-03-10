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
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Comparator;


/**
 * Merge Joins streamA with streamB based on the Comparator.
 * Supports:
 * one-to-one, one-to-many, many-to-one and many-to-many joins
 *
 **/

public class MergeJoinStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private PushBackStream streamA;
  private PushBackStream streamB;
  private Comparator<Tuple> comp;

  public MergeJoinStream(TupleStream streamA, TupleStream streamB, Comparator<Tuple> comp) {
    this.streamA = new PushBackStream(streamA);
    this.streamB = new PushBackStream(streamB);
    this.comp = comp;
  }

  public void setStreamContext(StreamContext context) {
    this.streamA.setStreamContext(context);
    this.streamB.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(streamA);
    l.add(streamB);
    return l;
  }

  public void open() throws IOException {
    streamA.open();
    streamB.open();
  }

  public void close() throws IOException {
    streamA.close();
    streamB.close();
  }

  private LinkedList<Tuple> joinTuples = new LinkedList();
  private List<Tuple> listA = new ArrayList();
  private List<Tuple> listB = new ArrayList();

  public Tuple read() throws IOException {

    if(joinTuples.size() > 0) {
      return joinTuples.removeFirst();
    }

    OUTER:
    while(true) {
      if(listA.size() == 0) {
        //Stream A needs to be advanced.
        Tuple a = streamA.read();
        if(a.EOF) {
          return a;
        }

        listA.add(a);
        INNERA:
        while(true) {
          Tuple a1 = streamA.read();
          if(a1.EOF) {
            streamA.pushBack(a1);
            break INNERA;
          }

          if(comp.compare(a,a1) == 0) {
            listA.add(a1);
          } else {
            streamA.pushBack(a1);
            break INNERA;
          }
        }
      }

      if(listB.size() == 0) {
        //StreamB needs to be advanced.
        Tuple b = streamB.read();
        if(b.EOF) {
          return b;
        }

        listB.add(b);
        INNERA:
        while(true) {
          Tuple b1 = streamB.read();

          if(b1.EOF) {
            streamB.pushBack(b1);
            break INNERA;
          }

          if(comp.compare(b,b1) == 0) {
            listB.add(b1);
          } else {
            streamB.pushBack(b1);
            break INNERA;
          }
        }
      }

      int c = comp.compare(listA.get(0),listB.get(0));
      if(c == 0) {
        //The Tuple lists match. So build all the Tuple combinations.
        for(Tuple aa : listA) {
          for(Tuple bb : listB) {
            Tuple clone = aa.clone();
            clone.fields.putAll(bb.fields);
            joinTuples.add(clone);
          }
        }

        //This will advance both streams.
        listA.clear();
        listB.clear();

        return joinTuples.removeFirst();
      } else if(c < 0) {
        //This will advance streamA
        listA.clear();
      } else {
        //This will advance streamB
        listB.clear();
      }
    }
  }

  public int getCost() {
    return 0;
  }
}