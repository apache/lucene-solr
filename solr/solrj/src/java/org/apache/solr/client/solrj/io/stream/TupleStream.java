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
package org.apache.solr.client.solrj.io.stream;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;


public abstract class TupleStream implements Closeable, Serializable, MapWriter {

  private static final long serialVersionUID = 1;
  
  private UUID streamNodeId = UUID.randomUUID();

  public TupleStream() {

  }
  public abstract void setStreamContext(StreamContext context);

  public abstract List<TupleStream> children();

  public abstract void open() throws IOException;

  public abstract void close() throws IOException;

  public abstract Tuple read() throws IOException;

  public abstract StreamComparator getStreamSort();
  
  public abstract Explanation toExplanation(StreamFactory factory) throws IOException;
  
  public int getCost() {
    return 0;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    open();
    ew.put("docs", (IteratorWriter) iw -> {
      try {
        for (; ; ) {
          Tuple tuple = read();
          if (tuple != null) {
            iw.add(tuple);
            if (tuple.EOF) {
              close();
              break;
            }
          } else {
            break;
          }
        }
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    });
  }

  public UUID getStreamNodeId(){
    return streamNodeId;
  }
}