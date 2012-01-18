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
import java.util.Random;

/**
 * 
 * A {@link DocumentsWriterPerThreadPool} that selects thread states at random.
 * 
 * @lucene.internal
 * @lucene.experimental
 */
public class RandomDocumentsWriterPerThreadPool extends
    DocumentsWriterPerThreadPool {
  private final ThreadState[] states;
  private final Random random;
  private final int maxRetry;

  public RandomDocumentsWriterPerThreadPool(int maxNumPerThreads, Random random) {
    super(maxNumPerThreads);
    assert getMaxThreadStates() >= 1;
    states = new ThreadState[maxNumPerThreads];
    this.random = new Random(random.nextLong());
    this.maxRetry = 1 + random.nextInt(10);
  }

  @Override
  public ThreadState getAndLock(Thread requestingThread,
      DocumentsWriter documentsWriter) {
    ThreadState threadState = null;
    if (getActiveThreadState() == 0) {
      synchronized (this) {
        if (getActiveThreadState() == 0) {
          threadState = states[0] = newThreadState();
          return threadState;
        }
      }
    }
    assert getActiveThreadState() > 0;
    for (int i = 0; i < maxRetry; i++) {
      int ord = random.nextInt(getActiveThreadState());
      synchronized (this) {
        threadState = states[ord];
        assert threadState != null;
      }

      if (threadState.tryLock()) {
        return threadState;
      }
      if (random.nextInt(20) == 0) {
        break;
      }
    }
    /*
     * only try to create a new threadstate if we can not lock the randomly
     * selected state. this is important since some tests rely on a single
     * threadstate in the single threaded case. Eventually it would be nice if
     * we would not have this limitation but for now we just make sure we only
     * allocate one threadstate if indexing is single threaded
     */

    synchronized (this) {
      ThreadState newThreadState = newThreadState();
      if (newThreadState != null) { // did we get a new state?
        threadState = states[getActiveThreadState() - 1] = newThreadState;
        assert threadState.isHeldByCurrentThread();
        return threadState;
      }
      // if no new state is available lock the random one
    }
    assert threadState != null;
    threadState.lock();
    return threadState;
  }

}
