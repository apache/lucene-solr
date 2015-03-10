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
import java.util.HashMap;
import java.util.LinkedList;

/**
 *
 *
 **/

public class HashJoinStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private PushBackStream streamA;
  private TupleStream streamB;
  private String[] keys;
  private HashMap<HashKey, List<Tuple>> hashMap = new HashMap();

  public HashJoinStream(TupleStream streamA, TupleStream streamB, String[] keys) {
    this.streamA = new PushBackStream(streamA);
    this.streamB = streamB;
    this.keys = keys;
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
    streamB.open();
    while(true) {
      Tuple t = streamB.read();
      if(t.EOF) {
        break;
      }

      HashKey hashKey = new HashKey(t, keys);
      if(hashMap.containsKey(hashKey)) {
        List<Tuple> tuples = hashMap.get(hashKey);
        tuples.add(t);
      } else {
        List<Tuple> tuples = new ArrayList();
        tuples.add(t);
        hashMap.put(hashKey, tuples);
      }
    }

    streamB.close();
    streamA.open();
  }

  public void close() throws IOException {
    streamA.close();
  }

  private LinkedList<Tuple> joinTuples = new LinkedList();

  public Tuple read() throws IOException {
    while(true) {
      Tuple tuple = streamA.read();

      if(tuple.EOF) {
        return tuple;
      }

      if(joinTuples.size() > 0) {
        Tuple t = tuple.clone();
        Tuple j = joinTuples.removeFirst();
        t.fields.putAll(j.fields);
        if(joinTuples.size() > 0) {
          streamA.pushBack(tuple);
        }

        return t;
      } else {
        HashKey hashKey = new HashKey(tuple, keys);

        if(hashMap.containsKey(hashKey)) {
          List<Tuple> joinWith = hashMap.get(hashKey);
          for(Tuple jt : joinWith) {
            joinTuples.add(jt);
          }
          streamA.pushBack(tuple);
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }
}