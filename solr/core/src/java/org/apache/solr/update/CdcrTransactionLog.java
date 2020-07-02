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
package org.apache.solr.update;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.Collection;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.update.processor.CdcrUpdateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends {@link org.apache.solr.update.TransactionLog} to:
 * <ul>
 * <li>reopen automatically the output stream if its reference count reached 0. This is achieved by extending
 * methods {@link #incref()}, {@link #close()} and {@link #reopenOutputStream()}.</li>
 * <li>encode the number of records in the tlog file in the last commit record. The number of records will be
 * decoded and reuse if the tlog file is reopened. This is achieved by extending the constructor, and the
 * methods {@link #writeCommit(CommitUpdateCommand)} and {@link #getReader(long)}.</li>
 * </ul>
 * @deprecated since 8.6
 */
@Deprecated
public class CdcrTransactionLog extends TransactionLog {

  private boolean isReplaying;
  long startVersion; // (absolute) version of the first element of this transaction log

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean debug = log.isDebugEnabled();

  CdcrTransactionLog(File tlogFile, Collection<String> globalStrings) {
    super(tlogFile, globalStrings);

    // The starting version number will be used to seek more efficiently tlogs
    // and to filter out tlog files during replication (in ReplicationHandler#getTlogFileList)
    String filename = tlogFile.getName();
    startVersion = Math.abs(Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1)));

    isReplaying = false;
  }

  CdcrTransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    super(tlogFile, globalStrings, openExisting);

    // The starting version number will be used to seek more efficiently tlogs
    String filename = tlogFile.getName();
    startVersion = Math.abs(Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1)));

    numRecords = openExisting ? this.readNumRecords() : 0;
    // if we try to reopen an existing tlog file and that the number of records is equal to 0, then we are replaying
    // the log and we will append a commit
    if (openExisting && numRecords == 0) {
      isReplaying = true;
    }
  }

  /**
   * Returns the number of records in the log (currently includes the header and an optional commit).
   */
  public int numRecords() {
    return super.numRecords();
  }

  /**
   * The last record of the transaction log file is expected to be a commit with a 4 byte integer that encodes the
   * number of records in the file.
   */
  private int readNumRecords() {
    try {
      if (endsWithCommit()) {
        long size = fos.size();
        // 4 bytes for the record size, the lenght of the end message + 1 byte for its value tag,
        // and 4 bytes for the number of records
        long pos = size - 4 - END_MESSAGE.length() - 1 - 4;
        if (pos < 0) return 0;
        try (ChannelFastInputStream is = new ChannelFastInputStream(channel, pos)) {
          return is.readInt();
        }
      }
    } catch (IOException e) {
      log.error("Error while reading number of records in tlog {}", this, e);
    }
    return 0;
  }

  @Override
  public long write(AddUpdateCommand cmd, long prevPointer) {
    assert (-1 <= prevPointer && (cmd.isInPlaceUpdate() || (-1 == prevPointer)));

    LogCodec codec = new LogCodec(resolver);
    SolrInputDocument sdoc = cmd.getSolrInputDocument();

    try {
      checkWriteHeader(codec, sdoc);

      // adaptive buffer sizing
      int bufSize = lastAddSize;    // unsynchronized access of lastAddSize should be fine
      bufSize = Math.min(1024*1024, bufSize+(bufSize>>3)+256);

      MemOutputStream out = new MemOutputStream(new byte[bufSize]);
      codec.init(out);
      if (cmd.isInPlaceUpdate()) {
        codec.writeTag(JavaBinCodec.ARR, 6);
        codec.writeInt(UpdateLog.UPDATE_INPLACE);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeLong(prevPointer);
        codec.writeLong(cmd.prevVersion);
        if (cmd.getReq().getParamString().contains(CdcrUpdateProcessor.CDCR_UPDATE)) {
          // if the update is received via cdcr source; add boolean entry
          // CdcrReplicator.isTargetCluster() checks that particular boolean to accept or discard the update
          // to forward to its own target cluster
          codec.writePrimitive(true);
        } else {
          codec.writePrimitive(false);
        }
        codec.writeSolrInputDocument(cmd.getSolrInputDocument());

      } else {
        codec.writeTag(JavaBinCodec.ARR, 4);
        codec.writeInt(UpdateLog.ADD);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        if (cmd.getReq().getParamString().contains(CdcrUpdateProcessor.CDCR_UPDATE)) {
          // if the update is received via cdcr source; add extra boolean entry
          // CdcrReplicator.isTargetCluster() checks that particular boolean to accept or discard the update
          // to forward to its own target cluster
          codec.writePrimitive(true);
        } else {
          codec.writePrimitive(false);
        }
        codec.writeSolrInputDocument(cmd.getSolrInputDocument());
      }
      lastAddSize = (int)out.size();

      synchronized (this) {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        assert pos != 0;

        /***
         System.out.println("###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         if (pos != fos.size()) {
         throw new RuntimeException("ERROR" + "###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         }
         ***/

        out.writeAll(fos);
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      }

    } catch (IOException e) {
      // TODO: reset our file pointer back to "pos", the start of this record.
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error logging add", e);
    }
  }

  @Override
  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);

    try {
      checkWriteHeader(codec, null);

      BytesRef br = cmd.getIndexedId();

      MemOutputStream out = new MemOutputStream(new byte[20 + br.length]);
      codec.init(out);
      codec.writeTag(JavaBinCodec.ARR, 4);
      codec.writeInt(UpdateLog.DELETE);  // should just take one byte
      codec.writeLong(cmd.getVersion());
      codec.writeByteArray(br.bytes, br.offset, br.length);
      if (cmd.getReq().getParamString().contains(CdcrUpdateProcessor.CDCR_UPDATE)) {
        // if the update is received via cdcr source; add extra boolean entry
        // CdcrReplicator.isTargetCluster() checks that particular boolean to accept or discard the update
        // to forward to its own target cluster
        codec.writePrimitive(true);
      } else {
        codec.writePrimitive(false);
      }

      synchronized (this) {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        assert pos != 0;
        out.writeAll(fos);
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    try {
      checkWriteHeader(codec, null);

      MemOutputStream out = new MemOutputStream(new byte[20 + (cmd.query.length())]);
      codec.init(out);
      codec.writeTag(JavaBinCodec.ARR, 4);
      codec.writeInt(UpdateLog.DELETE_BY_QUERY);  // should just take one byte
      codec.writeLong(cmd.getVersion());
      codec.writeStr(cmd.query);
      if (cmd.getReq().getParamString().contains(CdcrUpdateProcessor.CDCR_UPDATE)) {
        // if the update is received via cdcr source; add extra boolean entry
        // CdcrReplicator.isTargetCluster() checks that particular boolean to accept or discard the update
        // to forward to its own target cluster
        codec.writePrimitive(true);
      } else {
        codec.writePrimitive(false);
      }
      synchronized (this) {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        out.writeAll(fos);
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    synchronized (this) {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()

        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 4);
        codec.writeInt(UpdateLog.COMMIT);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeTag(JavaBinCodec.INT); // Enforce the encoding of a plain integer, to simplify decoding
        fos.writeInt(numRecords + 1); // the number of records in the file - +1 to account for the commit operation being written
        codec.writeStr(END_MESSAGE);  // ensure these bytes are (almost) last in the file

        endRecord(pos);

        fos.flush();  // flush since this will be the last record in a log fill
        assert fos.size() == channel.size();

        isReplaying = false; // we have replayed and appended a commit record with the number of records in the file

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  /**
   * Returns a reader that can be used while a log is still in use.
   * Currently only *one* LogReader may be outstanding, and that log may only
   * be used from a single thread.
   */
  @Override
  public LogReader getReader(long startingPos) {
    return new CdcrLogReader(startingPos);
  }

  public class CdcrLogReader extends LogReader {

    private int numRecords = 1; // start at 1 to account for the header record

    public CdcrLogReader(long startingPos) {
      super(startingPos);
    }

    @Override
    public Object next() throws IOException, InterruptedException {
      Object o = super.next();
      if (o != null) {
        this.numRecords++;
        // We are replaying the log. We need to update the number of records for the writeCommit.
        if (isReplaying) {
          synchronized (CdcrTransactionLog.this) {
            CdcrTransactionLog.this.numRecords = this.numRecords;
          }
        }
      }
      return o;
    }

  }

  @Override
  public void incref() {
    // if the refcount is 0, we need to reopen the output stream
    if (refcount.getAndIncrement() == 0) {
      reopenOutputStream(); // synchronised with this
    }
  }

  /**
   * Modified to act like {@link #incref()} in order to be compatible with {@link UpdateLog#recoverFromLog()}.
   * Otherwise, we would have to duplicate the method {@link UpdateLog#recoverFromLog()} in
   * {@link org.apache.solr.update.CdcrUpdateLog} and change the call
   * {@code if (!ll.try_incref()) continue; } to {@code incref(); }.
   */
  @Override
  public boolean try_incref() {
    this.incref();
    return true;
  }

  @Override
  public void close() {
    try {
      if (debug) {
        log.debug("Closing tlog {}", this);
      }

      synchronized (this) {
        if (fos != null) {
          fos.flush();
          fos.close();

          // dereference these variables for GC
          fos = null;
          os = null;
          channel = null;
          raf = null;
        }
      }

      if (deleteOnClose) {
        try {
          Files.deleteIfExists(tlogFile.toPath());
        } catch (IOException e) {
          // TODO: should this class care if a file couldnt be deleted?
          // this just emulates previous behavior, where only SecurityException would be handled.
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      assert ObjectReleaseTracker.release(this);
    }
  }

  /**
   * Re-open the output stream of the tlog and position
   * the file pointer at the end of the file. It assumes
   * that the tlog is non-empty and that the tlog's header
   * has been already read.
   */
  synchronized void reopenOutputStream() {
    try {
      if (debug) {
        log.debug("Re-opening tlog's output stream: {}", this);
      }

      raf = new RandomAccessFile(this.tlogFile, "rw");
      channel = raf.getChannel();
      long start = raf.length();
      raf.seek(start);
      os = Channels.newOutputStream(channel);
      fos = new FastOutputStream(os, new byte[65536], 0);
      fos.setWritten(start);    // reflect that we aren't starting at the beginning
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

}

