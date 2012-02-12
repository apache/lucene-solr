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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState; //javadoc

/**
 * A {@link DocumentsWriterPerThreadPool} implementation that tries to assign an
 * indexing thread to the same {@link ThreadState} each time the thread tries to
 * obtain a {@link ThreadState}. Once a new {@link ThreadState} is created it is
 * associated with the creating thread. Subsequently, if the threads associated
 * {@link ThreadState} is not in use it will be associated with the requesting
 * thread. Otherwise, if the {@link ThreadState} is used by another thread
 * {@link ThreadAffinityDocumentsWriterThreadPool} tries to find the currently
 * minimal contended {@link ThreadState}.
 */
public class ThreadAffinityDocumentsWriterThreadPool extends DocumentsWriterPerThreadPool {
  private Map<Thread, ThreadState> threadBindings = new ConcurrentHashMap<Thread, ThreadState>();
  
  /**
   * Creates a new {@link ThreadAffinityDocumentsWriterThreadPool} with a given maximum of {@link ThreadState}s.
   */
  public ThreadAffinityDocumentsWriterThreadPool(int maxNumPerThreads) {
    super(maxNumPerThreads);
    assert getMaxThreadStates() >= 1;
  }

  @Override
  public ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter) {
    ThreadState threadState = threadBindings.get(requestingThread);
    if (threadState != null && threadState.tryLock()) {
      return threadState;
    }
    ThreadState minThreadState = null;

    
    /* TODO -- another thread could lock the minThreadState we just got while 
     we should somehow prevent this. */
    // Find the state that has minimum number of threads waiting
    minThreadState = minContendedThreadState();
    if (minThreadState == null || minThreadState.hasQueuedThreads()) {
      final ThreadState newState = newThreadState(); // state is already locked if non-null
      if (newState != null) {
        assert newState.isHeldByCurrentThread();
        threadBindings.put(requestingThread, newState);
        return newState;
      } else if (minThreadState == null) {
        /*
         * no new threadState available we just take the minContented one
         * This must return a valid thread state since we accessed the 
         * synced context in newThreadState() above.
         */
        minThreadState = minContendedThreadState();
      }
    }
    assert minThreadState != null: "ThreadState is null";
    
    minThreadState.lock();
    return minThreadState;
  }
}
