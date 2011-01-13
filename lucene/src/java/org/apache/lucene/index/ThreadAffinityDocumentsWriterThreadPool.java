package org.apache.lucene.index;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.document.Document;

public class ThreadAffinityDocumentsWriterThreadPool extends DocumentsWriterPerThreadPool {
  private Map<Thread, ThreadState> threadBindings = new ConcurrentHashMap<Thread, ThreadState>();

  public ThreadAffinityDocumentsWriterThreadPool(int maxNumPerThreads) {
    super(maxNumPerThreads);
  }

  @Override
  public ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter, Document doc) {
    ThreadState threadState = threadBindings.get(requestingThread);
    if (threadState != null) {
      if (threadState.tryLock()) {
        return threadState;
      }
    }

    // find the state that has minimum amount of threads waiting
    Iterator<ThreadState> it = getActivePerThreadsIterator();
    ThreadState minThreadState = null;
    while (it.hasNext()) {
      ThreadState state = it.next();
      if (minThreadState == null || state.getQueueLength() < minThreadState.getQueueLength()) {
        minThreadState = state;
      }
    }

    if (minThreadState == null || minThreadState.hasQueuedThreads()) {
      ThreadState newState = newThreadState();
      if (newState != null) {
        minThreadState = newState;
        threadBindings.put(requestingThread, newState);
      }
    }

    minThreadState.lock();
    return minThreadState;
  }

  @Override
  public void clearThreadBindings(ThreadState perThread) {
    threadBindings.clear();
  }

  @Override
  public void clearAllThreadBindings() {
    threadBindings.clear();
  }
}
