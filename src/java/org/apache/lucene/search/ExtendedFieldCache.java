package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * This interface is obsolete, use {@link FieldCache} instead.
 * 
 * @deprecated Use {@link FieldCache}, this will be removed in Lucene 3.0
 **/
public interface ExtendedFieldCache extends FieldCache {
  
  /** @deprecated Use {@link FieldCache#DEFAULT}; this will be removed in Lucene 3.0 */
  public static ExtendedFieldCache EXT_DEFAULT = (ExtendedFieldCache) FieldCache.DEFAULT;
  
  /** @deprecated Use {@link FieldCache.LongParser}, this will be removed in Lucene 3.0 */
  public interface LongParser extends FieldCache.LongParser {
  }

  /** @deprecated Use {@link FieldCache.DoubleParser}, this will be removed in Lucene 3.0 */
  public interface DoubleParser extends FieldCache.DoubleParser {
  }

  /** @deprecated Will be removed in 3.0, this is for binary compatibility only */
  public long[] getLongs(IndexReader reader, String field, ExtendedFieldCache.LongParser parser)
        throws IOException;

  /** @deprecated Will be removed in 3.0, this is for binary compatibility only */
  public double[] getDoubles(IndexReader reader, String field, ExtendedFieldCache.DoubleParser parser)
        throws IOException;

}
