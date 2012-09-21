package org.apache.lucene.codecs;

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

import org.apache.lucene.index.DocValues; // javadocs
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentReadState;

/**
 * Encodes/decodes {@link DocValues}
 * @lucene.experimental
 */
public abstract class DocValuesFormat {
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocValuesFormat() {
  }

  /** Consumes (writes) doc values during indexing. */
  public abstract PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException;

  /** Produces (reads) doc values during reading/searching. */
  public abstract PerDocProducer docsProducer(SegmentReadState state) throws IOException;
}
