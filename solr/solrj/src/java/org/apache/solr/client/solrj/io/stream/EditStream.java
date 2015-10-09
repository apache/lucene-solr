package org.apache.solr.client.solrj.io.stream;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;

public class EditStream extends TupleStream {

  private static final long serialVersionUID = 1;
  private TupleStream stream;
  private List<String> remove;

  public EditStream(TupleStream stream, List<String> remove) {
    this.stream = stream;
    this.remove = remove;
  }

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    stream.open();
  }

  public void close() throws IOException {
    stream.close();
  }

  public Tuple read() throws IOException {
    Tuple tuple = stream.read();
    if(tuple.EOF) {
      return tuple;
    } else {
      for(String key : remove) {
        tuple.remove(key);
      }

      return tuple;
    }
  }

  public StreamComparator getStreamSort() {
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }
}