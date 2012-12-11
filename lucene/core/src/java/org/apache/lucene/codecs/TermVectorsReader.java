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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute; // javadocs
import org.apache.lucene.index.DocsAndPositionsEnum; // javadocs
import org.apache.lucene.index.Fields;

/**
 * Codec API for reading term vectors:
 * 
 * @lucene.experimental
 */
public abstract class TermVectorsReader implements Cloneable, Closeable {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected TermVectorsReader() {
  }

  /** Returns term vectors for this document, or null if
   *  term vectors were not indexed. If offsets are
   *  available they are in an {@link OffsetAttribute}
   *  available from the {@link DocsAndPositionsEnum}. */
  public abstract Fields get(int doc) throws IOException;

  /** Create a clone that one caller at a time may use to
   *  read term vectors. */
  @Override
  public abstract TermVectorsReader clone();
}
