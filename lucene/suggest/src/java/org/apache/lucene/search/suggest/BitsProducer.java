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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/** A producer of {@link Bits} per segment. */
public abstract class BitsProducer {

  /** Sole constructor, typically invoked by sub-classes. */
  protected BitsProducer() {}

  /**
   * Return {@link Bits} for the given leaf. The returned instance must be non-null and have a
   * {@link Bits#length() length} equal to {@link LeafReader#maxDoc() maxDoc}.
   */
  public abstract Bits getBits(LeafReaderContext context) throws IOException;
}
