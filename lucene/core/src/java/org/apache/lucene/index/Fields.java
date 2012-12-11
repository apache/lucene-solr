package org.apache.lucene.index;

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
import java.util.Iterator;

/** Flex API for access to fields and terms
 *  @lucene.experimental */

public abstract class Fields implements Iterable<String> {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected Fields() {
  }

  /** Returns an iterator that will step through all fields
   *  names.  This will not return null.  */
  @Override
  public abstract Iterator<String> iterator();

  /** Get the {@link Terms} for this field.  This will return
   *  null if the field does not exist. */
  public abstract Terms terms(String field) throws IOException;

  /** Returns the number of fields or -1 if the number of
   * distinct field names is unknown. If &gt;= 0,
   * {@link #iterator} will return as many field names. */
  public abstract int size();
  
  /** Returns the number of terms for all fields, or -1 if this 
   *  measure isn't stored by the codec. Note that, just like 
   *  other term measures, this measure does not take deleted 
   *  documents into account. 
   *  @deprecated iterate fields and add their size() instead. 
   *   this method is only provided as a transition mechanism
   *   to access this statistic for 3.x indexes, which do not
   *   have this statistic per-field.
   *  @see Terms#size() */
  @Deprecated
  public long getUniqueTermCount() throws IOException {
    long numTerms = 0;
    for (String field : this) {
      Terms terms = terms(field);
      if (terms != null) {
        final long termCount = terms.size();
        if (termCount == -1) {
          return -1;
        }
          
        numTerms += termCount;
      }
    }
    return numTerms;
  }

  /** Zero-length {@code Fields} array. */
  public final static Fields[] EMPTY_ARRAY = new Fields[0];
}
