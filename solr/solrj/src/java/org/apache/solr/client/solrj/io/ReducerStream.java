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
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;

/**
 *  Iterates over a TupleStream and buffers Tuples that are equal based on a field comparator.
 *  This allows tuples to be grouped by a common field.
 *
 *  The read() method emits one tuple per group. The top levels fields reflect the first tuple
 *  encountered in the group.
 *
 *  Use the Tuple.getMaps() method to return the all the Tuples in the group. The method returns
 *  a list of maps (including the group head), which hold the data for each Tuple in the group.
 *
 *  Note: This ReducerStream requires that the underlying streams be sorted and partitioned by same
 *  fields as it's comparator.
 *
 **/

public class ReducerStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private PushBackStream tupleStream;
  private Comparator<Tuple> comp;

  private Tuple currentGroupHead;

  public ReducerStream(TupleStream tupleStream,
                       Comparator<Tuple> comp) {
    this.tupleStream = new PushBackStream(tupleStream);
    this.comp = comp;
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

    List<Map> maps = new ArrayList();
    while(true) {
      Tuple t = tupleStream.read();

      if(t.EOF) {
       if(maps.size() > 0) {
         tupleStream.pushBack(t);
         Map map1 = maps.get(0);
         Map map2 = new HashMap();
         map2.putAll(map1);
         Tuple groupHead = new Tuple(map2);
         groupHead.setMaps(maps);
         return groupHead;
       } else {
         return t;
       }
      }

      if(currentGroupHead == null) {
        currentGroupHead = t;
        maps.add(t.getMap());
      } else {
        if(comp.compare(currentGroupHead, t) == 0) {
          maps.add(t.getMap());
        } else {
          Tuple groupHead = currentGroupHead.clone();
          tupleStream.pushBack(t);
          currentGroupHead = null;
          groupHead.setMaps(maps);
          return groupHead;
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }
}