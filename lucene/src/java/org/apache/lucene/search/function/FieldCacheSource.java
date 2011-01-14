package org.apache.lucene.search.function;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.FieldCache;

/**
 * Expert: A base class for ValueSource implementations that retrieve values for
 * a single field from the {@link org.apache.lucene.search.FieldCache FieldCache}.
 * <p>
 * Fields used herein must be indexed (doesn't matter if these fields are stored or not).
 * <p> 
 * It is assumed that each such indexed field is untokenized, or at least has a single token in a document.
 * For documents with multiple tokens of the same field, behavior is undefined (It is likely that current 
 * code would use the value of one of these tokens, but this is not guaranteed).
 * <p>
 * Document with no tokens in this field are assigned the <code>Zero</code> value.    
 * 
 * @lucene.experimental
 *
 * <p><b>NOTE</b>: with the switch in 2.9 to segment-based
 * searching, if {@link #getValues} is invoked with a
 * composite (multi-segment) reader, this can easily cause
 * double RAM usage for the values in the FieldCache.  It's
 * best to switch your application to pass only atomic
 * (single segment) readers to this API.</p>
 */
public abstract class FieldCacheSource extends ValueSource {
  private String field;

  /**
   * Create a cached field source for the input field.  
   */
  public FieldCacheSource(String field) {
    this.field=field;
  }

  /* (non-Javadoc) @see org.apache.lucene.search.function.ValueSource#getValues(org.apache.lucene.index.IndexReader) */
  @Override
  public final DocValues getValues(AtomicReaderContext context) throws IOException {
    return getCachedFieldValues(FieldCache.DEFAULT, field, context.reader);
  }

  /* (non-Javadoc) @see org.apache.lucene.search.function.ValueSource#description() */
  @Override
  public String description() {
    return field;
  }

  /**
   * Return cached DocValues for input field and reader.
   * @param cache FieldCache so that values of a field are loaded once per reader (RAM allowing)
   * @param field Field for which values are required.
   * @see ValueSource
   */
  public abstract DocValues getCachedFieldValues(FieldCache cache, String field, IndexReader reader) throws IOException;

  /*(non-Javadoc) @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof FieldCacheSource)) {
      return false;
    }
    FieldCacheSource other = (FieldCacheSource) o;
    return 
      this.field.equals(other.field) && 
      cachedFieldSourceEquals(other);
  }

  /*(non-Javadoc) @see java.lang.Object#hashCode() */
  @Override
  public final int hashCode() {
    return 
      field.hashCode() +
      cachedFieldSourceHashCode();
  }

  /**
   * Check if equals to another {@link FieldCacheSource}, already knowing that cache and field are equal.  
   * @see Object#equals(java.lang.Object)
   */
  public abstract boolean cachedFieldSourceEquals(FieldCacheSource other);

  /**
   * Return a hash code of a {@link FieldCacheSource}, without the hash-codes of the field 
   * and the cache (those are taken care of elsewhere).  
   * @see Object#hashCode()
   */
  public abstract int cachedFieldSourceHashCode();
}
