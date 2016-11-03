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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;


public abstract class TupleStream implements Closeable, Serializable, MapSerializable {

  private static final long serialVersionUID = 1;
  
  private UUID streamNodeId = UUID.randomUUID();

  public TupleStream() {

  }
/*
  public static void writeStreamOpen(Writer out) throws IOException {
    out.write("{\"docs\":[");
  }

  public static void writeStreamClose(Writer out) throws IOException {
    out.write("]}");
  }*/

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

  private boolean isOpen = false;

  @Override
  public Map toMap(Map<String, Object> map) {
    try {
      if (!isOpen) {
        open();
        isOpen = true;
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return Collections.singletonMap("docs", new Iterator<Tuple>() {
      Tuple tuple;
      boolean isEOF = false;

      @Override
      public boolean hasNext() {
        if (isEOF) return false;
        if (tuple != null) return true;
        try {
          tuple = read();
          if(tuple != null && tuple.EOF) close();
          return tuple != null;
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }

      @Override
      public Tuple next() {
        Tuple tmp = tuple;
        tuple = null;
        isEOF = tmp == null || tmp.EOF;
        return tmp;
      }
    });
  }

  public UUID getStreamNodeId(){
    return streamNodeId;
  }
}