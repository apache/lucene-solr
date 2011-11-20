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

import java.io.IOException;

import org.apache.lucene.util.AttributeSource;

/** Enumerates indexed fields.  You must first call {@link
 *  #next} before calling {@link #terms}.
 *
 * @lucene.experimental */

public abstract class FieldsEnum {

  // TODO: maybe allow retrieving FieldInfo for current
  // field, as optional method?

  private AttributeSource atts = null;

  /**
   * Returns the related attributes.
   */
  public AttributeSource attributes() {
    if (atts == null) {
      atts = new AttributeSource();
    }
    return atts;
  }
  
  /** Increments the enumeration to the next field. Returns
   * null when there are no more fields.*/
  public abstract String next() throws IOException;

  // TODO: would be nice to require/fix all impls so they
  // never return null here... we have to fix the writers to
  // never write 0-terms fields... or maybe allow a non-null
  // Terms instance in just this case

  /** Get {@link Terms} for the current field.  After {@link #next} returns
   *  null this method should not be called. This method may
   *  return null in some cases, which means the provided
   *  field does not have any terms. */
  public abstract Terms terms() throws IOException;

  // TODO: should we allow pulling Terms as well?  not just
  // the iterator?
  
  public final static FieldsEnum[] EMPTY_ARRAY = new FieldsEnum[0];

  /** Provides zero fields */
  public final static FieldsEnum EMPTY = new FieldsEnum() {

    @Override
    public String next() {
      return null;
    }

    @Override
    public Terms terms() {
      throw new IllegalStateException("this method should never be called");
    }
  };
}
