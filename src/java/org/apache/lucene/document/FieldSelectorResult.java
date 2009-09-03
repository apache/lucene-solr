package org.apache.lucene.document;

import java.io.Serializable;
/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *  Provides information about what should be done with this Field 
 *
 **/
//Replace with an enumerated type in 1.5
public final class FieldSelectorResult implements Serializable {

    /**
     * Load this {@link Field} every time the {@link Document} is loaded, reading in the data as it is encountered.
     *  {@link Document#getField(String)} and {@link Document#getFieldable(String)} should not return null.
     *<p/>
     * {@link Document#add(Fieldable)} should be called by the Reader.
      */
  public transient static final FieldSelectorResult LOAD = new FieldSelectorResult(0);
    /**
     * Lazily load this {@link Field}.  This means the {@link Field} is valid, but it may not actually contain its data until
     * invoked.  {@link Document#getField(String)} SHOULD NOT BE USED.  {@link Document#getFieldable(String)} is safe to use and should
     * return a valid instance of a {@link Fieldable}.
     *<p/>
     * {@link Document#add(Fieldable)} should be called by the Reader.
     */
  public transient static final FieldSelectorResult LAZY_LOAD = new FieldSelectorResult(1);
    /**
     * Do not load the {@link Field}.  {@link Document#getField(String)} and {@link Document#getFieldable(String)} should return null.
     * {@link Document#add(Fieldable)} is not called.
     * <p/>
     * {@link Document#add(Fieldable)} should not be called by the Reader.
     */
  public transient static final FieldSelectorResult NO_LOAD = new FieldSelectorResult(2);
    /**
     * Load this field as in the {@link #LOAD} case, but immediately return from {@link Field} loading for the {@link Document}.  Thus, the
     * Document may not have its complete set of Fields.  {@link Document#getField(String)} and {@link Document#getFieldable(String)} should
     * both be valid for this {@link Field}
     * <p/>
     * {@link Document#add(Fieldable)} should be called by the Reader.
     */
  public transient static final FieldSelectorResult LOAD_AND_BREAK = new FieldSelectorResult(3);
    /**
     * Behaves much like {@link #LOAD} but does not uncompress any compressed data.  This is used for internal purposes.
     * {@link Document#getField(String)} and {@link Document#getFieldable(String)} should not return null.
     * <p/>
     * {@link Document#add(Fieldable)} should be called by
     * the Reader.
     * @deprecated This is an internal option only, and is
     *    no longer needed now that {@link CompressionTools}
     *    is used for field compression.
     */
  public transient static final FieldSelectorResult LOAD_FOR_MERGE = new FieldSelectorResult(4);

     /** Expert:  Load the size of this {@link Field} rather than its value.
       * Size is measured as number of bytes required to store the field == bytes for a binary or any compressed value, and 2*chars for a String value.
      * The size is stored as a binary value, represented as an int in a byte[], with the higher order byte first in [0]
      */
  public transient static final FieldSelectorResult SIZE = new FieldSelectorResult(5);

  /** Expert: Like {@link #SIZE} but immediately break from the field loading loop, i.e., stop loading further fields, after the size is loaded */         
  public transient static final FieldSelectorResult SIZE_AND_BREAK = new FieldSelectorResult(6);



  private int id;

  private FieldSelectorResult(int id) {
    this.id = id;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final FieldSelectorResult that = (FieldSelectorResult) o;

    if (id != that.id) return false;

    return true;
  }

  public int hashCode() {
    return id;
  }
}
