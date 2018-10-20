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

package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.IOUtils;

/** Handles one set of files that need copying, either because we have a
 *  new NRT point, or we are pre-copying merged files for merge warming. */
class SimpleCopyJob extends CopyJob {
  final Connection c;

  final byte[] copyBuffer = new byte[65536];
  final CopyState copyState;

  private Iterator<Map.Entry<String,FileMetaData>> iter;

  public SimpleCopyJob(String reason, Connection c, CopyState copyState, SimpleReplicaNode dest, Map<String,FileMetaData> files, boolean highPriority, OnceDone onceDone)
    throws IOException {
    super(reason, files, dest, highPriority, onceDone);
    dest.message("create SimpleCopyJob o" + ord);
    this.c = c;
    this.copyState = copyState;
  }

  @Override
  public synchronized void start() throws IOException {
    if (iter == null) {
      iter = toCopy.iterator();

      // Send all file names / offsets up front to avoid ping-ping latency:
      try {

        // This means we resumed an already in-progress copy; we do this one first:
        if (current != null) {
          c.out.writeByte((byte) 0);
          c.out.writeString(current.name);
          c.out.writeVLong(current.getBytesCopied());
          totBytes += current.metaData.length;
        }

        for (Map.Entry<String,FileMetaData> ent : toCopy) {
          String fileName = ent.getKey();
          FileMetaData metaData = ent.getValue();
          totBytes += metaData.length;
          c.out.writeByte((byte) 0);
          c.out.writeString(fileName);
          c.out.writeVLong(0);
        }
        c.out.writeByte((byte) 1);
        c.flush();
        c.s.shutdownOutput();

        if (current != null) {
          // Do this only at the end, after sending all requested files, so we don't deadlock due to socket buffering waiting for primary to
          // send us this length:
          long len = c.in.readVLong();
          if (len != current.metaData.length) {
            throw new IllegalStateException("file " + current.name + ": meta data says length=" + current.metaData.length + " but c.in says " + len);
          }
        }

        dest.message("SimpleCopyJob.init: done start files count=" + toCopy.size() + " totBytes=" + totBytes);

      } catch (Throwable t) {
        cancel("exc during start", t);
        throw new NodeCommunicationException("exc during start", t);
      }
    } else {
      throw new IllegalStateException("already started");
    }
  }

  @Override
  public long getTotalBytesCopied() {
    return totBytesCopied;
  }

  @Override
  public Set<String> getFileNamesToCopy() {
    Set<String> fileNames = new HashSet<>();
    for(Map.Entry<String,FileMetaData> ent : toCopy) {
      fileNames.add(ent.getKey());
    }
    return fileNames;
  }

  @Override
  public Set<String> getFileNames() {
    return files.keySet();
  }

  /** Higher priority and then "first come first serve" order. */
  @Override
  public int compareTo(CopyJob _other) {
    SimpleCopyJob other = (SimpleCopyJob) _other;
    if (highPriority != other.highPriority) {
      return highPriority ? -1 : 1;
    } else if (ord < other.ord) {
      return -1;
    } else if (ord > other.ord) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public void finish() throws IOException {
    dest.message(String.format(Locale.ROOT,
                               "top: file copy done; took %.1f msec to copy %d bytes; now rename %d tmp files",
                               (System.nanoTime() - startNS)/1000000.0,
                               totBytesCopied,
                               copiedFiles.size()));

    // NOTE: if any of the files we copied overwrote a file in the current commit point, we (ReplicaNode) removed the commit point up
    // front so that the commit is not corrupt.  This way if we hit exc here, or if we crash here, we won't leave a corrupt commit in
    // the index:
    for(Map.Entry<String,String> ent : copiedFiles.entrySet()) {
      String tmpFileName = ent.getValue();
      String fileName = ent.getKey();

      if (Node.VERBOSE_FILES) {
        dest.message("rename file " + tmpFileName + " to " + fileName);
      }

      // NOTE: if this throws exception, then some files have been moved to their true names, and others are leftover .tmp files.  I don't
      // think heroic exception handling is necessary (no harm will come, except some leftover files),  nor warranted here (would make the
      // code more complex, for the exceptional cases when something is wrong w/ your IO system):
      dest.dir.rename(tmpFileName, fileName);
    }

    copiedFiles.clear();
  }

  /** Do an iota of work; returns true if all copying is done */
  synchronized boolean visit() throws IOException {
    if (exc != null) {
      // We were externally cancelled:
      return true;
    }

    if (current == null) {
      if (iter.hasNext() == false) {
        c.close();
        return true;
      }

      Map.Entry<String,FileMetaData> next = iter.next();
      FileMetaData metaData = next.getValue();
      String fileName = next.getKey();
      long len = c.in.readVLong();
      if (len != metaData.length) {
        throw new IllegalStateException("file " + fileName + ": meta data says length=" + metaData.length + " but c.in says " + len);
      }
      current = new CopyOneFile(c.in, dest, fileName, metaData, copyBuffer);
    }

    if (current.visit()) {
      // This file is done copying
      copiedFiles.put(current.name, current.tmpName);
      totBytesCopied += current.getBytesCopied();
      assert totBytesCopied <= totBytes: "totBytesCopied=" + totBytesCopied + " totBytes=" + totBytes;
      current = null;
      return false;
    }

    return false;
  }

  protected CopyOneFile newCopyOneFile(CopyOneFile prev) {
    return new CopyOneFile(prev, c.in);
  }

  @Override
  public synchronized void transferAndCancel(CopyJob prevJob) throws IOException {
    try {
      super.transferAndCancel(prevJob);
    } finally {
      IOUtils.closeWhileHandlingException(((SimpleCopyJob) prevJob).c);
    }
  }

  public synchronized void cancel(String reason, Throwable exc) throws IOException {
    try {
      super.cancel(reason, exc);
    } finally {
      IOUtils.closeWhileHandlingException(c);
    }
  }

  @Override
  public boolean getFailed() {
    return exc != null;
  }
  
  @Override
  public String toString() {
    return "SimpleCopyJob(ord=" + ord + " " + reason + " highPriority=" + highPriority + " files count=" + files.size() + " bytesCopied=" + totBytesCopied + " (of " + totBytes + ") filesCopied=" + copiedFiles.size() + ")";
  }

  @Override
  public void runBlocking() throws IOException {
    while (visit() == false);

    if (getFailed()) {
      throw new RuntimeException("copy failed: " + cancelReason, exc);
    }
  }

  @Override
  public CopyState getCopyState() {
    return copyState;
  }

  @Override
  public synchronized boolean conflicts(CopyJob _other) {
    Set<String> filesToCopy = new HashSet<>();
    for(Map.Entry<String,FileMetaData> ent : toCopy) {
      filesToCopy.add(ent.getKey());
    }

    SimpleCopyJob other = (SimpleCopyJob) _other;
    synchronized (other) {
      for(Map.Entry<String,FileMetaData> ent : other.toCopy) {
        if (filesToCopy.contains(ent.getKey())) {
          return true;
        }
      }
    }

    return false;
  }
}
