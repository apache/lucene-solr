package org.apache.lucene.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;

public class ThreadAffinityDocumentsWriterThreadPool extends DocumentsWriterThreadPool {
  private static final class AffinityThreadState extends ThreadState {
    int numAssignedThreads;

    @Override
    void finish() {
      numAssignedThreads--;
    }
  }

  private Map<Thread, AffinityThreadState> threadBindings = new HashMap<Thread, AffinityThreadState>();

  public ThreadAffinityDocumentsWriterThreadPool(int maxNumThreadStates) {
    super(maxNumThreadStates);
  }

  @Override
  protected ThreadState selectThreadState(Thread requestingThread, DocumentsWriter documentsWriter, Document doc) {
    AffinityThreadState threadState = threadBindings.get(requestingThread);
    // First, find a thread state.  If this thread already
    // has affinity to a specific ThreadState, use that one
    // again.
    if (threadState == null) {
      AffinityThreadState minThreadState = null;
      for(int i=0;i<allThreadStates.length;i++) {
        AffinityThreadState ts = (AffinityThreadState) allThreadStates[i];
        if (minThreadState == null || ts.numAssignedThreads < minThreadState.numAssignedThreads)
          minThreadState = ts;
      }
      if (minThreadState != null && (minThreadState.numAssignedThreads == 0 || allThreadStates.length >= maxNumThreadStates)) {
        threadState = minThreadState;
      } else {
        threadState = addNewThreadState(documentsWriter, new AffinityThreadState());
      }
      threadBindings.put(requestingThread, threadState);
    }
    threadState.numAssignedThreads++;

    return threadState;
  }

  @Override
  protected void clearThreadBindings(ThreadState flushedThread) {
    Iterator<Entry<Thread, AffinityThreadState>> it = threadBindings.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Thread, AffinityThreadState> e = it.next();
      if (e.getValue() == flushedThread) {
        it.remove();
      }
    }
  }

  @Override
  protected void clearAllThreadBindings() {
    threadBindings.clear();
  }
}
