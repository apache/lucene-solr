package org.apache.lucene.index;

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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.AttributeSource;

/** Iterates through the documents and term freqs.
 *  NOTE: you must first call {@link #nextDoc} before using
 *  any of the per-doc methods. */
public abstract class DocsEnum extends DocIdSetIterator {

  private AttributeSource atts = null;

  /** Returns term frequency in the current document.  Do
   *  not call this before {@link #nextDoc} is first called,
   *  nor after {@link #nextDoc} returns NO_MORE_DOCS. */
  public abstract int freq();
  
  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }
}
