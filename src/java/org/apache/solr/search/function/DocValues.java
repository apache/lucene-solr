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

package org.apache.solr.search.function;

import org.apache.lucene.search.Explanation;

/**
 * Represents field values as different types.
 * Normally created via a {@link ValueSource} for a particular field and reader.
 *
 * @version $Id$
 */

// DocValues is distinct from ValueSource because
// there needs to be an object created at query evaluation time that
// is not referenced by the query itself because:
// - Query objects should be MT safe
// - For caching, Query objects are often used as keys... you don't
//   want the Query carrying around big objects
public abstract class DocValues {
  public byte byteVal(int doc) { throw new UnsupportedOperationException(); }
  public short shortVal(int doc) { throw new UnsupportedOperationException(); }

  public float floatVal(int doc) { throw new UnsupportedOperationException(); }
  public int intVal(int doc) { throw new UnsupportedOperationException(); }
  public long longVal(int doc) { throw new UnsupportedOperationException(); }
  public double doubleVal(int doc) { throw new UnsupportedOperationException(); }
  public String strVal(int doc) { throw new UnsupportedOperationException(); }
  public abstract String toString(int doc);
  public Explanation explain(int doc) {
    return new Explanation(floatVal(doc), toString(doc));
  }
}
