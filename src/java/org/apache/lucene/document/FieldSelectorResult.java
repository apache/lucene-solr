package org.apache.lucene.document;
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
public final class FieldSelectorResult {

  public static final FieldSelectorResult LOAD = new FieldSelectorResult(0);
  public static final FieldSelectorResult LAZY_LOAD = new FieldSelectorResult(1);
  public static final FieldSelectorResult NO_LOAD = new FieldSelectorResult(2);
  public static final FieldSelectorResult LOAD_AND_BREAK = new FieldSelectorResult(3);
  
  private int id;

  private FieldSelectorResult(int id)
  {
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
