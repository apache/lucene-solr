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
package org.apache.lucene.codecs.cranky;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

class CrankyLiveDocsFormat extends LiveDocsFormat {
  final LiveDocsFormat delegate;
  final Random random;
  
  CrankyLiveDocsFormat(LiveDocsFormat delegate, Random random) {
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    return delegate.newLiveDocs(size);
  }

  @Override
  public MutableBits newLiveDocs(Bits existing) throws IOException {
    return delegate.newLiveDocs(existing);
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
    return delegate.readLiveDocs(dir, info, context);
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from LiveDocsFormat.writeLiveDocs()");
    }
    delegate.writeLiveDocs(bits, dir, info, newDelCount, context);
  }

  @Override
  public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
    // TODO: is this called only from write? if so we should throw exception!
    delegate.files(info, files);
  }
}
