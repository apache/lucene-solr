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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionStream extends TupleStream {

  private TupleStream stream;
  private Exception openException;
  private Logger log = LoggerFactory.getLogger(ExceptionStream.class);

  public ExceptionStream(TupleStream stream) {
    this.stream = stream;
  }

  public List<TupleStream> children() {
    return null;
  }

  public void open() {
    try {
      stream.open();
    } catch (Exception e) {
      this.openException = e;
    }
  }

  public Tuple read() {
    if(openException != null) {
      //There was an exception during the open.
      Map fields = new HashMap();
      fields.put("_EXCEPTION_", openException.getMessage());
      fields.put("EOF", true);
      log.error("Error while opening Stream", openException);
      return new Tuple(fields);
    }

    try {
      return stream.read();
    } catch (Exception e) {
      Map fields = new HashMap();
      fields.put("_EXCEPTION_", e.getMessage());
      fields.put("EOF", true);
      log.error("Error while reading Stream:" + e);
      return new Tuple(fields);
    }
  }

  public StreamComparator getStreamSort() {
    return this.stream.getStreamSort();
  }

  public void close() throws IOException {
    stream.close();
  }

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }
}
