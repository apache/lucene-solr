package org.apache.lucene.misc;

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

import org.apache.lucene.util.BytesRef;

public final class TermStats {
  public BytesRef termtext;
  public String field;
  public int docFreq;
  public long totalTermFreq;
  
  TermStats(String field, BytesRef termtext, int df) {
    this.termtext = BytesRef.deepCopyOf(termtext);
    this.field = field;
    this.docFreq = df;
  }
  
  TermStats(String field, BytesRef termtext, int df, long tf) {
    this.termtext = BytesRef.deepCopyOf(termtext);
    this.field = field;
    this.docFreq = df;
    this.totalTermFreq = tf;
  }
  
  String getTermText() {
    return termtext.utf8ToString();
  }

  @Override
  public String toString() {
    return("TermStats: term=" + termtext.utf8ToString() + " docFreq=" + docFreq + " totalTermFreq=" + totalTermFreq);
  }
}