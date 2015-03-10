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
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

public class UniqueStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private Comparator<Tuple> comp;
  private Tuple currentTuple = null;

  public UniqueStream(TupleStream tupleStream, Comparator<Tuple> comp) {
    this.tupleStream = tupleStream;
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
    Tuple tuple = tupleStream.read();
    if(tuple.EOF) {
      return tuple;
    }

    if(currentTuple == null) {
      currentTuple = tuple;
      return tuple;
    } else {
      while(true) {
        int i = comp.compare(currentTuple, tuple);
        if(i == 0) {
          //We have duplicate tuple so read the next tuple from the stream.
          tuple = tupleStream.read();
          if(tuple.EOF) {
            return tuple;
          }
        } else {
          //We have a non duplicate
          this.currentTuple = tuple;
          return tuple;
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }
}