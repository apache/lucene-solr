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

import org.apache.lucene.index.DocsEnum; // javadocs
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.TermState;

/**
 * Holds all state required for {@link PostingsReaderBase}
 * to produce a {@link DocsEnum} without re-seeking the
 * terms dict.
 */
public class PrefixCodedTermState extends OrdTermState {
  public int docFreq; // how many docs have this term
  public long filePointer; // fp into the terms dict primary file (_X.tis)
  public long totalTermFreq;                           // total number of occurrences of this term
  
  @Override
  public void copyFrom(TermState _other) {
    assert _other instanceof PrefixCodedTermState : "can not copy from " + _other.getClass().getName();
    PrefixCodedTermState other = (PrefixCodedTermState) _other;
    super.copyFrom(_other);
    filePointer = other.filePointer;
    docFreq = other.docFreq;
    totalTermFreq = other.totalTermFreq;
  }

  @Override
  public String toString() {
    return super.toString() + "[ord=" + ord + ", tis.filePointer=" + filePointer + ", docFreq=" + docFreq + ", totalTermFreq=" + totalTermFreq + "]";
  }
  
}
