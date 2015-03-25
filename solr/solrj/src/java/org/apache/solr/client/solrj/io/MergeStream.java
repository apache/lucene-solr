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

/**
* Unions streamA with streamB ordering the Tuples based on a Comparator.
* Both streams must be sorted by the fields being compared.
**/


public class MergeStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private PushBackStream streamA;
  private PushBackStream streamB;
  private Comparator<Tuple> comp;

  public MergeStream(TupleStream streamA, TupleStream streamB, Comparator<Tuple> comp) {
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

  public Tuple read() throws IOException {
    Tuple a = streamA.read();
    Tuple b = streamB.read();

    if(a.EOF && b.EOF) {
      return a;
    }

    if(a.EOF) {
      streamA.pushBack(a);
      return b;
    }

    if(b.EOF) {
      streamB.pushBack(b);
      return a;
    }

    int c = comp.compare(a,b);

    if(c < 0) {
      streamB.pushBack(b);
      return a;
    } else {
      streamA.pushBack(a);
      return b;
    }
  }

  public int getCost() {
    return 0;
  }
}