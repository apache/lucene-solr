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

import org.apache.lucene.document.Document;

// nocommit jdoc
// nocommit -- can/should apps set this via IWC
public class ThreadAffinityDocumentsWriterThreadPool extends DocumentsWriterPerThreadPool {
  private Map<Thread, ThreadState> threadBindings = new ConcurrentHashMap<Thread, ThreadState>();

  public ThreadAffinityDocumentsWriterThreadPool(int maxNumPerThreads) {
    super(maxNumPerThreads);
    assert getMaxThreadStates() >= 1;
  }

  @Override
  public ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter, Document doc) {
    ThreadState threadState = threadBindings.get(requestingThread);
    if (threadState != null) {
      if (threadState.tryLock()) {
        return threadState;
      }
    }
    ThreadState minThreadState = null;

    // Find the state that has minimum number of threads waiting
    // noocommit -- can't another thread lock the
    // minThreadState we just got?
    minThreadState = minContendedThreadState();

    if (minThreadState == null || minThreadState.hasQueuedThreads()) {
      ThreadState newState = newThreadState();
      if (newState != null) {
        minThreadState = newState;
        threadBindings.put(requestingThread, newState);
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

  /*
  @Override
  public void clearThreadBindings(ThreadState perThread) {
    threadBindings.clear();
  }

  @Override
  public void clearAllThreadBindings() {
    threadBindings.clear();
  }
  */
}
