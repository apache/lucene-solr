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
package org.apache.lucene.index;

/**
 * A callback event listener for recording key events happened inside IndexWriter
 *
 * @lucene.experimental
 */
public interface IndexWriterEventListener {
  /** A no-op listener that helps to save null checks */
  IndexWriterEventListener NO_OP_LISTENER =
      new IndexWriterEventListener() {
        @Override
        public void beginMergeOnFullFlush(MergePolicy.OneMerge merge) {}

        @Override
        public void endMergeOnFullFlush(MergePolicy.OneMerge merge) {}
      };

  /** Invoked at the start of merge on commit */
  void beginMergeOnFullFlush(MergePolicy.OneMerge merge);

  /**
   * Invoked at the end of merge on commit, due to either merge completed, or merge timed out
   * according to {@link IndexWriterConfig#setMaxFullFlushMergeWaitMillis(long)}
   */
  void endMergeOnFullFlush(MergePolicy.OneMerge merge);
}
