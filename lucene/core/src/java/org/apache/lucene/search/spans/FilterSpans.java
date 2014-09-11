package org.apache.lucene.search.spans;

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
import java.util.Collection;

/**
 * A {@link Spans} implementation which allows wrapping another spans instance
 * and override some selected methods.
 */
public class FilterSpans extends Spans {
 
  /** The wrapped spans instance. */
  protected final Spans in;
  
  /** Wrap the given {@link Spans}. */
  public FilterSpans(Spans in) {
    this.in = in;
  }
  
  @Override
  public boolean next() throws IOException {
    return in.next();
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    return in.skipTo(target);
  }

  @Override
  public int doc() {
    return in.doc();
  }

  @Override
  public int start() {
    return in.start();
  }

  @Override
  public int end() {
    return in.end();
  }
  
  @Override
  public Collection<byte[]> getPayload() throws IOException {
    return in.getPayload();
  }

  @Override
  public boolean isPayloadAvailable() throws IOException {
    return in.isPayloadAvailable();
  }
  
  @Override
  public long cost() {
    return in.cost();
  }
  
  @Override
  public String toString() {
    return "Filter(" + in.toString() + ")";
  }
  
}
