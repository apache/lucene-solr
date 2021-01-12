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

package org.apache.lucene.luke.models.overview;

import org.apache.lucene.luke.util.BytesRefUtils;

/** Holder for statistics for a term in a specific field. */
public final class TermStats {

  private final String decodedTermText;

  private final String field;

  private final int docFreq;

  /**
   * Returns a TermStats instance representing the specified {@link
   * org.apache.lucene.misc.TermStats} value.
   */
  static TermStats of(org.apache.lucene.misc.TermStats stats) {
    String termText = BytesRefUtils.decode(stats.termtext);
    return new TermStats(termText, stats.field, stats.docFreq);
  }

  private TermStats(String decodedTermText, String field, int docFreq) {
    this.decodedTermText = decodedTermText;
    this.field = field;
    this.docFreq = docFreq;
  }

  /** Returns the string representation for this term. */
  public String getDecodedTermText() {
    return decodedTermText;
  }

  /** Returns the field name. */
  public String getField() {
    return field;
  }

  /** Returns the document frequency of this term. */
  public int getDocFreq() {
    return docFreq;
  }

  @Override
  public String toString() {
    return "TermStats{"
        + "decodedTermText='"
        + decodedTermText
        + '\''
        + ", field='"
        + field
        + '\''
        + ", docFreq="
        + docFreq
        + '}';
  }
}
