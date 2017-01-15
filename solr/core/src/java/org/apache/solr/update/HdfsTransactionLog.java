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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.util.FSHDFSUtils;
import org.apache.solr.util.FSHDFSUtils.CallerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Log Format: List{Operation, Version, ...}
 *  ADD, VERSION, DOC
 *  DELETE, VERSION, ID_BYTES
 *  DELETE_BY_QUERY, VERSION, String
 *
 *  TODO: keep two files, one for [operation, version, id] and the other for the actual
 *  document data.  That way we could throw away document log files more readily
 *  while retaining the smaller operation log files longer (and we can retrieve
 *  the stored fields from the latest documents from the index).
 *
 *  This would require keeping all source fields stored of course.
 *
 *  This would also allow to not log document data for requests with commit=true
 *  in them (since we know that if the request succeeds, all docs will be committed)
 *
 */
public class HdfsTransactionLog extends TransactionLog {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static boolean debug = log.isDebugEnabled();
  private static boolean trace = log.isTraceEnabled();


  Path tlogFile;

  private long finalLogSize;
  private FSDataOutputStream tlogOutStream;
  private FileSystem fs;

  private volatile boolean isClosed = false;

  HdfsTransactionLog(FileSystem fs, Path tlogFile, Collection<String> globalStrings, Integer tlogDfsReplication) {
    this(fs, tlogFile, globalStrings, false, tlogDfsReplication);
  }

  HdfsTransactionLog(FileSystem fs, Path tlogFile, Collection<String> globalStrings, boolean openExisting, Integer tlogDfsReplication) {
    super();
    boolean success = false;
    this.fs = fs;

    try {
      if (debug) {
        //log.debug("New TransactionLog file=" + tlogFile + ", exists=" + tlogFile.exists() + ", size=" + tlogFile.length() + ", openExisting=" + openExisting);
      }
      this.tlogFile = tlogFile;
      
      if (fs.exists(tlogFile) && openExisting) {
        FSHDFSUtils.recoverFileLease(fs, tlogFile, fs.getConf(), new CallerInfo(){

          @Override
          public boolean isCallerClosed() {
            return isClosed;
          }});
        
        tlogOutStream = fs.append(tlogFile);
      } else {
        fs.delete(tlogFile, false);
        
        tlogOutStream = fs.create(tlogFile, (short)tlogDfsReplication.intValue());
        tlogOutStream.hsync();
      }

      fos = new FastOutputStream(tlogOutStream, new byte[65536], 0);
      long start = tlogOutStream.getPos(); 

      if (openExisting) {
        if (start > 0) {
          readHeader(null);
          
         // we should already be at the end 
         // raf.seek(start);

        //  assert channel.position() == start;
          fos.setWritten(start);    // reflect that we aren't starting at the beginning
          //assert fos.size() == channel.size();
        } else {
          addGlobalStrings(globalStrings);
        }
      } else {
        if (start > 0) {
          log.error("New transaction log already exists:" + tlogFile + " size=" + tlogOutStream.size());
        }

        addGlobalStrings(globalStrings);
      }

      success = true;

      assert ObjectReleaseTracker.track(this);
      log.debug("Opening new tlog {}", this);
      
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      if (!success && tlogOutStream != null) {
        try {
          tlogOutStream.close();
        } catch (Exception e) {
          log.error("Error closing tlog file (after error opening)", e);
        }
      }
    }
  }

  @Override
  public boolean endsWithCommit() throws IOException {
    ensureFlushed();
    long size = getLogSize();
    
    // the end of the file should have the end message (added during a commit) plus a 4 byte size
    byte[] buf = new byte[ END_MESSAGE.length() ];
    long pos = size - END_MESSAGE.length() - 4;
    if (pos < 0) return false;
    
    FSDataFastInputStream dis = new FSDataFastInputStream(fs.open(tlogFile), pos);
    try {
      dis.read(buf);
      for (int i=0; i<buf.length; i++) {
        if (buf[i] != END_MESSAGE.charAt(i)) return false;
      }
    } finally {
      dis.close();
    }
    return true;
  }
  
  // This could mess with any readers or reverse readers that are open, or anything that might try to do a log lookup.
  // This should only be used to roll back buffered updates, not actually applied updates.
  @Override
  public void rollback(long pos) throws IOException {
    synchronized (this) {
      assert snapshot_size == pos;
      ensureFlushed();
      // TODO: how do we rollback with hdfs?? We need HDFS-3107
      fos.setWritten(pos);
      assert fos.size() == pos;
      numRecords = snapshot_numRecords;
    }
  }

  private void readHeader(FastInputStream fis) throws IOException {
    // read existing header
    boolean closeFis = false;
    if (fis == null) closeFis = true;
    fis = fis != null ? fis : new FSDataFastInputStream(fs.open(tlogFile), 0);
    Map header = null;
    try {
      LogCodec codec = new LogCodec(resolver);
      header = (Map) codec.unmarshal(fis);
      
      fis.readInt(); // skip size
    } finally {
      if (fis != null && closeFis) {
        fis.close();
      }
    }
    // needed to read other records

    synchronized (this) {
      globalStringList = (List<String>)header.get("strings");
      globalStringMap = new HashMap<>(globalStringList.size());
      for (int i=0; i<globalStringList.size(); i++) {
        globalStringMap.put( globalStringList.get(i), i+1);
      }
    }
  }

  @Override
  public long writeCommit(CommitUpdateCommand cmd, int flags) {
    LogCodec codec = new LogCodec(resolver);
    synchronized (this) {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()

        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.COMMIT | flags);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(END_MESSAGE);  // ensure these bytes are (almost) last in the file

        endRecord(pos);
        
        ensureFlushed();  // flush since this will be the last record in a log fill

        // now the commit command is written we will never write to this log again
        closeOutput();

        //assert fos.size() == channel.size();

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }


  /* This method is thread safe */
  @Override
  public Object lookup(long pos) {
    // A negative position can result from a log replay (which does not re-log, but does
    // update the version map.  This is OK since the node won't be ACTIVE when this happens.
    if (pos < 0) return null;

    try {
      // make sure any unflushed buffer has been flushed
      ensureFlushed();

      FSDataFastInputStream dis = new FSDataFastInputStream(fs.open(tlogFile),
          pos);
      try {
        dis.seek(pos);
        LogCodec codec = new LogCodec(resolver);
        return codec.readVal(new FastInputStream(dis));
      } finally {
        dis.close();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "pos=" + pos, e);
    }
  }

  @Override
  public void closeOutput() {
    try {
      doCloseOutput();
    } catch (IOException e) {
      log.error("Could not close tlog output", e);
      // This situation is not fatal to the caller
    }
  }

  private void doCloseOutput() throws IOException {
    synchronized (this) {
      if (fos == null) return;
      if (debug) {
        log.debug("Closing output for " + tlogFile);
      }
      fos.flushBuffer();
      finalLogSize = fos.size();
      fos = null;
    }

    tlogOutStream.hflush();
    tlogOutStream.close();
    tlogOutStream = null;
  }

  private void ensureFlushed() throws IOException {
    synchronized (this) {
      if (fos != null) {
        fos.flush();
        tlogOutStream.hflush();
      }
    }
  }

  @Override
  public long getLogSize() {
    synchronized (this) {
      if (fos != null) {
        return fos.size();
      } else {
        return finalLogSize;
      }
    }
  }

  @Override
  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE) return;
    try {
      synchronized (this) {
        fos.flushBuffer();
      }

      if (syncLevel == UpdateLog.SyncLevel.FSYNC) {
        tlogOutStream.hsync();
      } else {
        tlogOutStream.hflush();
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
  
  @Override
  public void close() {
    try {
      if (debug) {
        log.debug("Closing tlog {}", this);
      }

      doCloseOutput();

    } catch (IOException e) {
      log.error("Exception closing tlog.", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      isClosed  = true;
      assert ObjectReleaseTracker.release(this);
      if (deleteOnClose) {
        try {
          fs.delete(tlogFile, true);
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
    }
  }

  public String toString() {
    return "hdfs tlog{file=" + tlogFile.toString() + " refcount=" + refcount.get() + "}";
  }

  /** Returns a reader that can be used while a log is still in use.
   * Currently only *one* LogReader may be outstanding, and that log may only
   * be used from a single thread. */
  @Override
  public LogReader getReader(long startingPos) {
    return new HDFSLogReader(startingPos);
  }

  /** Returns a single threaded reverse reader */
  @Override
  public ReverseReader getReverseReader() throws IOException {
    return new HDFSReverseReader();
  }


  public class HDFSLogReader extends LogReader{
    FSDataFastInputStream fis;
    private LogCodec codec = new LogCodec(resolver);
    private long sz;

    public HDFSLogReader(long startingPos) {
      super();
      incref();
      initStream(startingPos);
    }

    private void initStream(long pos) {
      try {
        
        synchronized (HdfsTransactionLog.this) {
          ensureFlushed();
          sz = getLogSize();
        }

        FSDataInputStream fdis = fs.open(tlogFile);
        fis = new FSDataFastInputStream(fdis, pos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException, InterruptedException {
      long pos = fis.position();

      synchronized (HdfsTransactionLog.this) {
        if (trace) {
          log.trace("Reading log record.  pos="+pos+" currentSize="+getLogSize());
        }

        if (pos >= getLogSize()) {
          return null;
        }
      }
      
      // we actually need a new reader to 
      // see if any data was added by the writer
      if (pos >= sz) {
        log.info("Read available inputstream data, opening new inputstream pos={} sz={}", pos, sz);
        
        fis.close();
        initStream(pos);
      }
      
      if (pos == 0) {
        readHeader(fis);

        // shouldn't currently happen - header and first record are currently written at the same time
        synchronized (HdfsTransactionLog.this) {
          if (fis.position() >= getLogSize()) {
            return null;
          }
          pos = fis.position();
        }
      }

      Object o = codec.readVal(fis);

      // skip over record size
      int size = fis.readInt();
      assert size == fis.position() - pos - 4;

      return o;
    }

    public void close() {
      try {
        fis.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      decref();
    }

    @Override
    public String toString() {
      synchronized (HdfsTransactionLog.this) {
        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + getLogSize() + "}";
      }
    }
    
    @Override
    public long currentPos() {
      return fis.position();
    }
    
    @Override
    public long currentSize() {
      return getLogSize();
    }

  }

  public class HDFSReverseReader extends ReverseReader {
    FSDataFastInputStream fis;
    private LogCodec codec = new LogCodec(resolver) {
      @Override
      public SolrInputDocument readSolrInputDocument(DataInputInputStream dis) {
        // Given that the SolrInputDocument is last in an add record, it's OK to just skip
        // reading it completely.
        return null;
      }
    };

    int nextLength;  // length of the next record (the next one closer to the start of the log file)
    long prevPos;    // where we started reading from last time (so prevPos - nextLength == start of next record)

    public HDFSReverseReader() throws IOException {
      incref();

      long sz;
      synchronized (HdfsTransactionLog.this) {
        ensureFlushed();
        sz = getLogSize();
      }

      fis = new FSDataFastInputStream(fs.open(tlogFile), 0);
      
      if (sz >=4) {
        // readHeader(fis);  // should not be needed
        prevPos = sz - 4;
        fis.seek(prevPos);
        nextLength = fis.readInt();
      }
    }


    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException {
      if (prevPos <= 0) return null;

      long endOfThisRecord = prevPos;

      int thisLength = nextLength;

      long recordStart = prevPos - thisLength;  // back up to the beginning of the next record
      prevPos = recordStart - 4;  // back up 4 more to read the length of the next record

      if (prevPos <= 0) return null;  // this record is the header

      long bufferPos = fis.getBufferPos();
      if (prevPos >= bufferPos) {
        // nothing to do... we're within the current buffer
      } else {
        // Position buffer so that this record is at the end.
        // For small records, this will cause subsequent calls to next() to be within the buffer.
        long seekPos =  endOfThisRecord - fis.getBufferSize();
        seekPos = Math.min(seekPos, prevPos); // seek to the start of the record if it's larger then the block size.
        seekPos = Math.max(seekPos, 0);
        fis.seek(seekPos);
        fis.peek();  // cause buffer to be filled
      }

      fis.seek(prevPos);
      nextLength = fis.readInt();     // this is the length of the *next* record (i.e. closer to the beginning)

      // TODO: optionally skip document data
      Object o = codec.readVal(fis);

      // assert fis.position() == prevPos + 4 + thisLength;  // this is only true if we read all the data (and we currently skip reading SolrInputDocument
      return o;
    }

    /* returns the position in the log file of the last record returned by next() */
    public long position() {
      return prevPos + 4;  // skip the length
    }

    public void close() {
      try {
        fis.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      decref();
    }

    @Override
    public String toString() {
      synchronized (HdfsTransactionLog.this) {
        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + getLogSize() + "}";
      }
    }


  }

}



class FSDataFastInputStream extends FastInputStream {
  private FSDataInputStream fis;

  public FSDataFastInputStream(FSDataInputStream fis, long chPosition) {
    // super(null, new byte[10],0,0);    // a small buffer size for testing purposes
    super(null);
    this.fis = fis;
    super.readFromStream = chPosition;
  }

  @Override
  public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
    return fis.read(readFromStream, target, offset, len);
  }

  public void seek(long position) throws IOException {
    if (position <= readFromStream && position >= getBufferPos()) {
      // seek within buffer
      pos = (int)(position - getBufferPos());
    } else {
      // long currSize = ch.size();   // not needed - underlying read should handle (unless read never done)
      // if (position > currSize) throw new EOFException("Read past EOF: seeking to " + position + " on file of size " + currSize + " file=" + ch);
      readFromStream = position;
      end = pos = 0;
    }
    assert position() == position;
  }

  /** where is the start of the buffer relative to the whole file */
  public long getBufferPos() {
    return readFromStream - end;
  }

  public int getBufferSize() {
    return buf.length;
  }

  @Override
  public void close() throws IOException {
    fis.close();
  }
  
  @Override
  public String toString() {
    return "readFromStream="+readFromStream +" pos="+pos +" end="+end + " bufferPos="+getBufferPos() + " position="+position() ;
  }
}
