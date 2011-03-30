package org.apache.lucene.index;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfos.FieldNumberBiMap;
import org.apache.lucene.index.SegmentCodecs.SegmentCodecsBuilder;
import org.apache.lucene.index.codecs.CodecProvider;

public abstract class DocumentsWriterPerThreadPool {
  final static class ThreadState extends ReentrantLock {
    final DocumentsWriterPerThread perThread;

    ThreadState(DocumentsWriterPerThread perThread) {
      this.perThread = perThread;
    }
  }

  private final ThreadState[] perThreads;
  private volatile int numThreadStatesActive;

  public DocumentsWriterPerThreadPool(int maxNumPerThreads) {
    maxNumPerThreads = (maxNumPerThreads < 1) ? IndexWriterConfig.DEFAULT_MAX_THREAD_STATES : maxNumPerThreads;
    this.perThreads = new ThreadState[maxNumPerThreads];

    numThreadStatesActive = 0;
  }

  public void initialize(DocumentsWriter documentsWriter, FieldNumberBiMap globalFieldMap, IndexWriterConfig config) {
    final CodecProvider codecProvider = config.getCodecProvider();
    for (int i = 0; i < perThreads.length; i++) {
      final FieldInfos infos = globalFieldMap.newFieldInfos(SegmentCodecsBuilder.create(codecProvider));
      perThreads[i] = new ThreadState(new DocumentsWriterPerThread(documentsWriter.directory, documentsWriter, infos, documentsWriter.chain));
    }
  }

  public int getMaxThreadStates() {
    return perThreads.length;
  }

  public synchronized ThreadState newThreadState() {
    if (numThreadStatesActive < perThreads.length) {
      ThreadState state = perThreads[numThreadStatesActive];
      numThreadStatesActive++;
      return state;
    }

    return null;
  }

  public abstract ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter, Document doc);

  public abstract void clearThreadBindings(ThreadState perThread);

  public abstract void clearAllThreadBindings();

  public Iterator<ThreadState> getAllPerThreadsIterator() {
    return getPerThreadsIterator(this.perThreads.length);
  }

  public Iterator<ThreadState> getActivePerThreadsIterator() {
    return getPerThreadsIterator(this.numThreadStatesActive);
  }

  private Iterator<ThreadState> getPerThreadsIterator(final int upto) {
    return new Iterator<ThreadState>() {
      int i = 0;

      public boolean hasNext() {
        return i < upto;
      }

      public ThreadState next() {
        return perThreads[i++];
      }

      public void remove() {
        throw new UnsupportedOperationException("remove() not supported.");
      }
    };
  }
}
