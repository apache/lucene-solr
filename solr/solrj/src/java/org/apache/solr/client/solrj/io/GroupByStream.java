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
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 *  Iterates over a TupleStream Groups The TopN Tuples of a group.
 **/

public class GroupByStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private Comparator<Tuple> interGroupComp;
  private Comparator<Tuple> intraGroupComp;
  private Comparator<Tuple> reverseComp;
  private Tuple currentTuple;
  private int size;

  public GroupByStream(TupleStream tupleStream,
                       Comparator<Tuple> interGroupComp,
                       Comparator<Tuple> intraGroupComp,
                       int size) {
    this.tupleStream = tupleStream;
    this.interGroupComp = interGroupComp;
    this.intraGroupComp = intraGroupComp;
    this.reverseComp = new ReverseComp(intraGroupComp);
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
    currentTuple = tupleStream.read(); //Read the first Tuple so currentTuple is never null;
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {

    if(currentTuple.EOF) {
      return currentTuple;
    }

    PriorityQueue<Tuple> group = new PriorityQueue<>(size, reverseComp);
    group.add(currentTuple);
    while(true) {
      Tuple t = tupleStream.read();

      if(t.EOF) {
        currentTuple = t;
        break;
      }

      if(interGroupComp.compare(currentTuple, t) == 0) {
        if(group.size() >= size) {
          Tuple peek = group.peek();
          if(intraGroupComp.compare(t, peek) < 0) {
            group.poll();
            group.add(t);
          }
        } else {
          group.add(t);
        }
      } else {
        currentTuple = t;
        break;
      }
    }

    //We have a finished group so add the Tuples to an array.
    Tuple[] members = new Tuple[group.size()];
    for(int i=group.size()-1; i>=0; i--) {
      Tuple t = group.poll();
      members[i] = t;
    }

    //First Tuple is the group head.
    Tuple groupHead = members[0];
    if(members.length > 1) {
      List groupList = new ArrayList();
      for(int i=1; i<members.length; i++) {
        groupList.add(members[i].fields);
      }

      groupHead.set("tuples", groupList);
    } else {
      groupHead.set("tuples", new ArrayList());
    }
    return groupHead;
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