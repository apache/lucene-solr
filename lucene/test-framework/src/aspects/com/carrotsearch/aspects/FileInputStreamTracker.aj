package com.carrotsearch.aspects;

import static com.carrotsearch.aspects.Tracker.isTracking;
import static com.carrotsearch.aspects.Tracker.track;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;

/** */
public aspect FileInputStreamTracker {
  /** */
  public final class Delegate extends FileInputStream {
    private String path;
    
    public Delegate(File file) throws FileNotFoundException {
      super(file);
      if (LuceneTestCase.VERBOSE) System.out.println("Opening FIS(" + file.getAbsolutePath() + ")");
      this.path = file.getAbsolutePath();
    }

    public Delegate(String path) throws FileNotFoundException { 
      super(path);
      if (LuceneTestCase.VERBOSE) System.out.println("Opening FIS(" + path + ")");
      this.path = path;
    }

    public Delegate(FileDescriptor fd) { 
      super(fd);
      if (LuceneTestCase.VERBOSE) System.out.println("Opening FIS(" + fd + ")");
      this.path = "[FileDescriptor constructor]";
    }

    @Override
    public void close() throws IOException {
      // TODO: Mark as closed, even if an exception occurs in real close()?
      super.close();
      Tracker.close(this);
    }
    
    @Override
    protected void finalize() throws IOException {
      // We do not rely in finalize() closing the stream, so close the actual
      // stream, but do NOT mark it as properly closed.
      super.close();
    }
    
    @Override
    public String toString() {
      return "FileInputStream(" + path + ")";
    }
  }

  /** */
  FileInputStream around(File file) throws FileNotFoundException: 
    call(FileInputStream.new(File) throws FileNotFoundException) && 
    args(file)
  {
    return isTracking() ? track(new Delegate(file)) : proceed(file);
  }
  
  /** */
  FileInputStream around(String path) throws FileNotFoundException: 
    call(FileInputStream.new(String) throws FileNotFoundException) && 
    args(path)
  {
    return isTracking() ? track(new Delegate(path)) : proceed(path);
  }

  /** */
  FileInputStream around(FileDescriptor fd): 
    call(FileInputStream.new(FileDescriptor)) && 
    args(fd)
  {
    return isTracking() ? track(new Delegate(fd)) : proceed(fd);
  }

  /** 
   * Tracking any other classes inheriting from FileInputStream is tricky because we don't know
   * their code; we'd need to make sure they override close() and finalize() so that we can 
   * advise the code!
   * 
   * For now, just ban such classes at runtime (prevent instantiation).
   */
  before():
    !within(com.carrotsearch.aspects.**) &&
    execution(FileInputStream+.new(..)) {
    try {
      ((FileInputStream) thisJoinPoint.getTarget()).close();
    } catch (IOException e) {/* Try to close the super, we can't inject before it. */}
    throw new UnsupportedOperationException("FileInputStream not supported for classes that inherit from FileInputStream.");
  }
}
