package org.apache.lucene.codecs;

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
import java.util.Set;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

/** Format for live/deleted documents
 * @lucene.experimental */
public abstract class LiveDocsFormat {
  /** creates a new mutablebits, with all bits set, for the specified size */
  public abstract MutableBits newLiveDocs(int size) throws IOException;
  /** creates a new mutablebits of the same bits set and size of existing */
  public abstract MutableBits newLiveDocs(Bits existing) throws IOException;
  /** reads bits from a file */
  public abstract Bits readLiveDocs(Directory dir, SegmentInfo info, IOContext context) throws IOException;
  /** writes bits to a file */
  public abstract void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfo info, IOContext context) throws IOException;
  public abstract void files(SegmentInfo info, Set<String> files) throws IOException;
}
