package org.apache.solr.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class RefCntRamDirectory extends RAMDirectory {

  private final AtomicInteger refCount = new AtomicInteger();

  public RefCntRamDirectory() {
    super();
    refCount.set(1);
  }

  public RefCntRamDirectory(Directory dir) throws IOException {
    this();
    for (String file : dir.listAll()) {
      dir.copy(this, file, file);
    }
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
    decRef();
  }

  public boolean isOpen() {
    return isOpen;
  }

}
