package org.apache.solr.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class RefCntRamDirectory extends RAMDirectory {
  
  private final AtomicInteger refCount = new AtomicInteger();
  
  public RefCntRamDirectory() {
    super();
    incRef();
  }

  public RefCntRamDirectory(Directory dir) throws IOException {
    this();
    Directory.copy(dir, this, false);
  }
  
  public void incRef() {
    ensureOpen();
    refCount.incrementAndGet();
  }
  
  public void decRef() {
    ensureOpen();
    if (refCount.getAndDecrement() == 1) {
      super.close();
    }
  }
  
  public final synchronized void close() {
    if (isOpen) {
      decRef();
    }
  }
  
  public boolean isOpen() {
    return isOpen;
  }

}
