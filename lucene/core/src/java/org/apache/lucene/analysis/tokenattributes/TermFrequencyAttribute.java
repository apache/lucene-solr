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

package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.Attribute;

/** Sets the custom term frequency of a term within one document.  If this attribute
 *  is present in your analysis chain for a given field, that field must be indexed with
 *  {@link IndexOptions#DOCS_AND_FREQS}. */
public interface TermFrequencyAttribute extends Attribute {

  /** Set the custom term frequency of the current term within one document. */
  public void setTermFrequency(int termFrequency);

  /** Returns the custom term frequencey. */
  public int getTermFrequency();
}
