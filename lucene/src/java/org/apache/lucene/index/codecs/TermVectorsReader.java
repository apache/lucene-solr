package org.apache.lucene.index.codecs;

/**
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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermVectorMapper;

public abstract class TermVectorsReader implements Cloneable,Closeable {
  // TODO: can we consolidate all these get's?
  public abstract TermFreqVector[] get(int doc) throws IOException;
  public abstract TermFreqVector get(int doc, String field) throws IOException;
  
  public abstract void get(int doc, TermVectorMapper mapper) throws IOException;
  public abstract void get(int doc, String field, TermVectorMapper mapper) throws IOException;

  public abstract TermVectorsReader clone();
}
