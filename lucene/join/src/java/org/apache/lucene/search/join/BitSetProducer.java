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
package org.apache.lucene.search.join;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;

/** A producer of {@link BitSet}s per segment. */
public interface BitSetProducer {

  /**
   * Produce a {@link BitSet} matching the expected documents on the given segment. This may return
   * {@code null} if no documents match.
   */
  BitSet getBitSet(LeafReaderContext context) throws IOException;
}
