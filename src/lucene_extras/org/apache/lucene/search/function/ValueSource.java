/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.function.DocValues;

import java.io.IOException;
import java.io.Serializable;

/**
 * Instantiates {@link org.apache.lucene.search.function.DocValues} for a particular reader.
 * <br>
 * Often used when creating a {@link FunctionQuery}.
 *
 * @author yonik
 * @version $Id: ValueSource.java,v 1.2 2005/11/30 19:31:01 yonik Exp $
 */
public abstract class ValueSource implements Serializable {

  public abstract DocValues getValues(IndexReader reader) throws IOException;

  public abstract boolean equals(Object o);

  public abstract int hashCode();

  /** description of field, used in explain() */
  public abstract String description();

  public String toString() {
    return getClass().getName() + ":" + description();
  }

}
