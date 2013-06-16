package org.apache.lucene.codecs.temp;
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

import java.util.Arrays;

import org.apache.lucene.index.DocsEnum; // javadocs
import org.apache.lucene.codecs.TempPostingsReaderBase; // javadocs
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.ByteArrayDataInput;

/**
 * Holds all state required for {@link TempPostingsReaderBase}
 * to produce a {@link DocsEnum} without re-seeking the
 * terms dict.
 */
public class TempTermState extends TermState {
  /** how many docs have this term */
  public int docFreq;
  /** total number of occurrences of this term */
  public long totalTermFreq;

  /** the term's ord in the current block */
  public int termBlockOrd;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected TempTermState() {
  }

  @Override
  public void copyFrom(TermState _other) {
    assert _other instanceof TempTermState : "can not copy from " + _other.getClass().getName();
    TempTermState other = (TempTermState) _other;
    docFreq = other.docFreq;
    totalTermFreq = other.totalTermFreq;
    termBlockOrd = other.termBlockOrd;
  }

  @Override
  public String toString() {
    return "docFreq=" + docFreq + " totalTermFreq=" + totalTermFreq + " termBlockOrd=" + termBlockOrd;
  }
}
