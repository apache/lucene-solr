package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;

/**
 * Default {@link FlushPolicy} implementation that flushes based on RAM
 * Consumption, document count and number of buffered deletes depending on the
 * IndexWriters {@link IndexWriterConfig}. This {@link FlushPolicy} will only
 * respect settings which are not disabled during initialization (
 * {@link #init(DocumentsWriter)}). All enabled {@link IndexWriterConfig}
 * settings are used to mark {@link DocumentsWriterPerThread} as flush pending
 * during indexing with respect to thier live updates.
 * <p>
 * If {@link IndexWriterConfig#setRAMBufferSizeMB(double)} is enabled always the
 * largest ram consuming {@link DocumentsWriterPerThread} will be marked as
 * pending iff the global active RAM consumption is equals or higher the
 * configured max RAM buffer.
 */
public class FlushByRamOrCountsPolicy extends FlushPolicy {

  @Override
  public void onDelete(DocumentsWriterFlushControl control, ThreadState state) {
    if (flushOnDeleteTerms()) {
      final int maxBufferedDeleteTerms = indexWriterConfig
          .getMaxBufferedDeleteTerms();
      if (control.getNumGlobalTermDeletes() >= maxBufferedDeleteTerms) {
        control.setFlushDeletes();
      }
    }
  }

  @Override
  public void onInsert(DocumentsWriterFlushControl control, ThreadState state) {
    if (flushOnDocCount()
        && state.perThread.getNumDocsInRAM() >= indexWriterConfig
            .getMaxBufferedDocs()) {
      control.setFlushPending(state); // flush by num docs
    } else {// flush by RAM
      if (flushOnRAM()) {
        final double ramBufferSizeMB = indexWriterConfig.getRAMBufferSizeMB();
        final long totalRam = control.activeBytes();
        final long limit = (long) (ramBufferSizeMB * 1024.d * 1024.d);
        if (totalRam >= limit) {
          markLargestWriterPending(control, state, totalRam);
        }
      }
    }
  }
}
