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

import org.apache.lucene.index.DocsEnum;          // for javadocs

import org.apache.lucene.index.codecs.standard.StandardPostingsReader; // javadocs

/**
 * Holds all state required for {@link StandardPostingsReader}
 * to produce a {@link DocsEnum} without re-seeking the
 * terms dict.
 * @lucene.experimental
 */

public class TermState implements Cloneable {
  public long ord;                                     // ord for this term
  public long filePointer;                             // fp into the terms dict primary file (_X.tis)
  public int docFreq;                                  // how many docs have this term

  public void copy(TermState other) {
    ord = other.ord;
    filePointer = other.filePointer;
    docFreq = other.docFreq;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // should not happen
      throw new RuntimeException(cnse);
    }
  }

  @Override
  public String toString() {
    return "tis.fp=" + filePointer + " docFreq=" + docFreq + " ord=" + ord;
  }
}
