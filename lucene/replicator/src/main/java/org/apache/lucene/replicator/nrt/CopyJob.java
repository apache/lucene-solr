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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.IOUtils;

/** Handles copying one set of files, e.g. all files for a new NRT point, or files for pre-copying a merged segment.
 *  This notifies the caller via OnceDone when the job finishes or failed.
 *
 * @lucene.experimental */
public abstract class CopyJob implements Comparable<CopyJob> {
  private final static AtomicLong counter = new AtomicLong();
  protected final ReplicaNode dest;

  protected final Map<String,FileMetaData> files;

  public final long ord = counter.incrementAndGet();

  /** True for an NRT sync, false for pre-copying a newly merged segment */
  public final boolean highPriority;

  public final OnceDone onceDone;

  public final long startNS = System.nanoTime();

  public final String reason;

  protected final List<Map.Entry<String,FileMetaData>> toCopy;

  protected long totBytes;

  protected long totBytesCopied;

  // The file we are currently copying:
  protected CopyOneFile current;

  // Set when we are cancelled
  protected volatile Throwable exc;
  protected volatile String cancelReason;

  // toString may concurrently access this:
  protected final Map<String,String> copiedFiles = new ConcurrentHashMap<>();

  protected CopyJob(String reason, Map<String,FileMetaData> files, ReplicaNode dest, boolean highPriority, OnceDone onceDone) throws IOException {
    this.reason = reason;
    this.files = files;
    this.dest = dest;
    this.highPriority = highPriority;
    this.onceDone = onceDone;

    // Exceptions in here are bad:
    try {
      this.toCopy = dest.getFilesToCopy(this.files);
    } catch (Throwable t) {
      cancel("exc during init", t);
      throw new CorruptIndexException("exception while checking local files", "n/a", t);
    }
  }

  /** Callback invoked by CopyJob once all files have (finally) finished copying */
  public interface OnceDone {
    public void run(CopyJob job) throws IOException;
  }

  /** Transfers whatever tmp files were already copied in this previous job and cancels the previous job */
  public synchronized void transferAndCancel(CopyJob prevJob) throws IOException {
    synchronized(prevJob) {
      dest.message("CopyJob: now transfer prevJob " + prevJob);
      try {
        _transferAndCancel(prevJob);
      } catch (Throwable t) {
        dest.message("xfer: exc during transferAndCancel");
        cancel("exc during transferAndCancel", t);
        throw IOUtils.rethrowAlways(t);
      }
    }
  }

  private synchronized void _transferAndCancel(CopyJob prevJob) throws IOException {

    // Caller must already be sync'd on prevJob:
    assert Thread.holdsLock(prevJob);

    if (prevJob.exc != null) {
      // Already cancelled
      dest.message("xfer: prevJob was already cancelled; skip transfer");
      return;
    }

    // Cancel the previous job
    prevJob.exc = new Throwable();

    // Carry over already copied files that we also want to copy
    Iterator<Map.Entry<String,FileMetaData>> it = toCopy.iterator();
    long bytesAlreadyCopied = 0;

    // Iterate over all files we think we need to copy:
    while (it.hasNext()) {
      Map.Entry<String,FileMetaData> ent = it.next();
      String fileName = ent.getKey();
      String prevTmpFileName = prevJob.copiedFiles.get(fileName);
      if (prevTmpFileName != null) {
        // This fileName is common to both jobs, and the old job already finished copying it (to a temp file), so we keep it:
        long fileLength = ent.getValue().length;
        bytesAlreadyCopied += fileLength;
        dest.message("xfer: carry over already-copied file " + fileName + " (" + prevTmpFileName + ", " + fileLength + " bytes)");
        copiedFiles.put(fileName, prevTmpFileName);

        // So we don't try to delete it, below:
        prevJob.copiedFiles.remove(fileName);

        // So it's not in our copy list anymore:
        it.remove();
      } else if (prevJob.current != null && prevJob.current.name.equals(fileName)) {
        // This fileName is common to both jobs, and it's the file that the previous job was in the process of copying.  In this case
        // we continue copying it from the prevoius job.  This is important for cases where we are copying over a large file
        // because otherwise we could keep failing the NRT copy and restarting this file from the beginning and never catch up:
        dest.message("xfer: carry over in-progress file " + fileName + " (" + prevJob.current.tmpName + ") bytesCopied=" + prevJob.current.getBytesCopied() + " of " + prevJob.current.bytesToCopy);
        bytesAlreadyCopied += prevJob.current.getBytesCopied();

        assert current == null;

        // must set current first, before writing/read to c.in/out in case that hits an exception, so that we then close the temp
        // IndexOutput when cancelling ourselves:
        current = newCopyOneFile(prevJob.current);

        // Tell our new (primary) connection we'd like to copy this file first, but resuming from how many bytes we already copied last time:
        // We do this even if bytesToCopy == bytesCopied, because we still need to readLong() the checksum from the primary connection:
        assert prevJob.current.getBytesCopied() <= prevJob.current.bytesToCopy;

        prevJob.current = null;

        totBytes += current.metaData.length;

        // So it's not in our copy list anymore:
        it.remove();
      } else {
        dest.message("xfer: file " + fileName + " will be fully copied");
      }
    }
    dest.message("xfer: " + bytesAlreadyCopied + " bytes already copied of " + totBytes);

    // Delete all temp files the old job wrote but we don't need:
    dest.message("xfer: now delete old temp files: " + prevJob.copiedFiles.values());
    IOUtils.deleteFilesIgnoringExceptions(dest.dir, prevJob.copiedFiles.values());

    if (prevJob.current != null) { 
      IOUtils.closeWhileHandlingException(prevJob.current);
      if (Node.VERBOSE_FILES) {
        dest.message("remove partial file " + prevJob.current.tmpName);
      }
      dest.deleter.deleteNewFile(prevJob.current.tmpName);
      prevJob.current = null;
    }
  }

  protected abstract CopyOneFile newCopyOneFile(CopyOneFile current);

  /** Begin copying files */
  public abstract void start() throws IOException;

  /** Use current thread (blocking) to do all copying and then return once done, or throw exception on failure */
  public abstract void runBlocking() throws Exception;

  public void cancel(String reason, Throwable exc) throws IOException {
    if (this.exc != null) {
      // Already cancelled
      return;
    }

    dest.message(String.format(Locale.ROOT, "top: cancel after copying %s; exc=%s:\n  files=%s\n  copiedFiles=%s",
                               Node.bytesToString(totBytesCopied),
                               exc,
                               files == null ? "null" : files.keySet(), copiedFiles.keySet()));

    if (exc == null) {
      exc = new Throwable();
    }

    this.exc = exc;
    this.cancelReason = reason;

    // Delete all temp files we wrote:
    IOUtils.deleteFilesIgnoringExceptions(dest.dir, copiedFiles.values());

    if (current != null) { 
      IOUtils.closeWhileHandlingException(current);
      if (Node.VERBOSE_FILES) {
        dest.message("remove partial file " + current.tmpName);
      }
      dest.deleter.deleteNewFile(current.tmpName);
      current = null;
    }
  }

  /** Return true if this job is trying to copy any of the same files as the other job */
  public abstract boolean conflicts(CopyJob other);

  /** Renames all copied (tmp) files to their true file names */
  public abstract void finish() throws IOException;

  public abstract boolean getFailed();

  /** Returns only those file names (a subset of {@link #getFileNames}) that need to be copied */
  public abstract Set<String> getFileNamesToCopy();

  /** Returns all file names referenced in this copy job */
  public abstract Set<String> getFileNames();

  public abstract CopyState getCopyState();

  public abstract long getTotalBytesCopied();
}
