/**
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

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.rmi.registry.LocateRegistry;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
public class TransactionLog {
  public static Logger log = LoggerFactory.getLogger(TransactionLog.class);

  public final static String END_MESSAGE="SOLR_TLOG_END";

  long id;
  File tlogFile;
  RandomAccessFile raf;
  FileChannel channel;
  OutputStream os;
  FastOutputStream fos;    // all accesses to this stream should be synchronized on "this" (The TransactionLog)
  int numRecords;
  
  volatile boolean deleteOnClose = true;  // we can delete old tlogs since they are currently only used for real-time-get (and in the future, recovery)

  AtomicInteger refcount = new AtomicInteger(1);
  Map<String,Integer> globalStringMap = new HashMap<String, Integer>();
  List<String> globalStringList = new ArrayList<String>();
  final boolean debug = log.isDebugEnabled();

  long snapshot_size;
  int snapshot_numRecords;
  
  // write a BytesRef as a byte array
  JavaBinCodec.ObjectResolver resolver = new JavaBinCodec.ObjectResolver() {
    @Override
    public Object resolve(Object o, JavaBinCodec codec) throws IOException {
      if (o instanceof BytesRef) {
        BytesRef br = (BytesRef)o;
        codec.writeByteArray(br.bytes, br.offset, br.length);
        return null;
      }
      return o;
    }
  };

  public class LogCodec extends JavaBinCodec {
    public LogCodec() {
      super(resolver);
    }

    @Override
    public void writeExternString(String s) throws IOException {
      if (s == null) {
        writeTag(NULL);
        return;
      }

      // no need to synchronize globalStringMap - it's only updated before the first record is written to the log
      Integer idx = globalStringMap.get(s);
      if (idx == null) {
        // write a normal string
        writeStr(s);
      } else {
        // write the extern string
        writeTag(EXTERN_STRING, idx);
      }
    }

    @Override
    public String readExternString(FastInputStream fis) throws IOException {
      int idx = readSize(fis);
      if (idx != 0) {// idx != 0 is the index of the extern string
      // no need to synchronize globalStringList - it's only updated before the first record is written to the log
        return globalStringList.get(idx - 1);
      } else {// idx == 0 means it has a string value
        // this shouldn't happen with this codec subclass.
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Corrupt transaction log");
      }
    }


  }


  TransactionLog(File tlogFile, Collection<String> globalStrings) throws IOException {
    this(tlogFile, globalStrings, false);
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) throws IOException {
    try {
      if (debug) {
        log.debug("New TransactionLog file=" + tlogFile + ", exists=" + tlogFile.exists() + ", size=" + tlogFile.length() + ", openExisting=" + openExisting);
      }

      this.tlogFile = tlogFile;
      raf = new RandomAccessFile(this.tlogFile, "rw");
      long start = raf.length();
      channel = raf.getChannel();
      os = Channels.newOutputStream(channel);
      fos = FastOutputStream.wrap(os);

      if (openExisting) {
        if (start > 0) {
          readHeader(null);
          raf.seek(start);
          assert channel.position() == start;
          fos.setWritten(start);    // reflect that we aren't starting at the beginning
          assert fos.size() == channel.size();
        } else {
          addGlobalStrings(globalStrings);
        }
      } else {
        assert start==0;
        if (start > 0) {
          raf.setLength(0);
        }
        addGlobalStrings(globalStrings);
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /** Returns the number of records in the log (currently includes the header and an optional commit).
   * Note: currently returns 0 for reopened existing log files.
   */
  public int numRecords() {
    synchronized (this) {
      return this.numRecords;
    }
  }

  public boolean endsWithCommit() throws IOException {
    long size;
    synchronized (this) {
      fos.flush();
      size = fos.size();
    }

    
    // the end of the file should have the end message (added during a commit) plus a 4 byte size
    byte[] buf = new byte[ END_MESSAGE.length() ];
    long pos = size - END_MESSAGE.length() - 4;
    if (pos < 0) return false;
    ChannelFastInputStream is = new ChannelFastInputStream(channel, pos);
    is.read(buf);
    for (int i=0; i<buf.length; i++) {
      if (buf[i] != END_MESSAGE.charAt(i)) return false;
    }
    return true;
  }

  /** takes a snapshot of the current position and number of records
   * for later possible rollback, and returns the position */
  public long snapshot() {
    synchronized (this) {
      snapshot_size = fos.size();
      snapshot_numRecords = numRecords;
      return snapshot_size;
    }    
  }
  
  // This could mess with any readers or reverse readers that are open, or anything that might try to do a log lookup.
  // This should only be used to roll back buffered updates, not actually applied updates.
  public void rollback(long pos) throws IOException {
    synchronized (this) {
      assert snapshot_size == pos;
      fos.flush();
      raf.setLength(pos);
      fos.setWritten(pos);
      assert fos.size() == pos;
      numRecords = snapshot_numRecords;
    }
  }


  public long writeData(Object o) {
    LogCodec codec = new LogCodec();
    try {
      long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
      codec.init(fos);
      codec.writeVal(o);
      return pos;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }


  private void readHeader(FastInputStream fis) throws IOException {
    // read existing header
    fis = fis != null ? fis : new ChannelFastInputStream(channel, 0);
    LogCodec codec = new LogCodec();
    Map header = (Map)codec.unmarshal(fis);

    fis.readInt(); // skip size

    // needed to read other records

    synchronized (this) {
      globalStringList = (List<String>)header.get("strings");
      globalStringMap = new HashMap<String, Integer>(globalStringList.size());
      for (int i=0; i<globalStringList.size(); i++) {
        globalStringMap.put( globalStringList.get(i), i+1);
      }
    }
  }

  private void addGlobalStrings(Collection<String> strings) {
    if (strings == null) return;
    int origSize = globalStringMap.size();
    for (String s : strings) {
      Integer idx = null;
      if (origSize > 0) {
        idx = globalStringMap.get(s);
      }
      if (idx != null) continue;  // already in list
      globalStringList.add(s);
      globalStringMap.put(s, globalStringList.size());
    }
    assert globalStringMap.size() == globalStringList.size();
  }

  Collection<String> getGlobalStrings() {
    synchronized (this) {
      return new ArrayList<String>(globalStringList);
    }
  }

  private void writeLogHeader(LogCodec codec) throws IOException {
    long pos = fos.size();
    assert pos == 0;

    Map header = new LinkedHashMap<String,Object>();
    header.put("SOLR_TLOG",1); // a magic string + version number
    header.put("strings",globalStringList);
    codec.marshal(header, fos);

    endRecord(pos);
  }

  private void endRecord(long startRecordPosition) throws IOException {
    fos.writeInt((int)(fos.size() - startRecordPosition));
    numRecords++;
  }


  public long write(AddUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    long pos = 0;
    synchronized (this) {
      try {
        pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        SolrInputDocument sdoc = cmd.getSolrInputDocument();

        if (pos == 0) { // TODO: needs to be changed if we start writing a header first
          addGlobalStrings(sdoc.getFieldNames());
          writeLogHeader(codec);
          pos = fos.size();
        }

        /***
        System.out.println("###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         if (pos != fos.size()) {
          throw new RuntimeException("ERROR" + "###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
        }
         ***/

        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.ADD);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeSolrInputDocument(cmd.getSolrInputDocument());

        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      } catch (IOException e) {
        // TODO: reset our file pointer back to "pos", the start of this record.
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error logging add", e);
      }
    }
  }

  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (this) {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.DELETE);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        BytesRef br = cmd.getIndexedId();
        codec.writeByteArray(br.bytes, br.offset, br.length);

        endRecord(pos);
        // fos.flushBuffer();  // flush later

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (this) {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.DELETE_BY_QUERY);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(cmd.query);

        endRecord(pos);
        // fos.flushBuffer();  // flush later

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }


  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (this) {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()

        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.COMMIT);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(END_MESSAGE);  // ensure these bytes are (almost) last in the file

        endRecord(pos);
        
        fos.flush();  // flush since this will be the last record in a log fill
        assert fos.size() == channel.size();

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }


  /* This method is thread safe */
  public Object lookup(long pos) {
    // A negative position can result from a log replay (which does not re-log, but does
    // update the version map.  This is OK since the node won't be ACTIVE when this happens.
    if (pos < 0) return null;

    try {
      // make sure any unflushed buffer has been flushed
      synchronized (this) {
        // TODO: optimize this by keeping track of what we have flushed up to
        fos.flushBuffer();
        /***
         System.out.println("###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
        if (fos.size() != raf.length() || pos >= fos.size() ) {
          throw new RuntimeException("ERROR" + "###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
        }
        ***/
      }

      ChannelFastInputStream fis = new ChannelFastInputStream(channel, pos);
      LogCodec codec = new LogCodec();
      return codec.readVal(fis);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void incref() {
    int result = refcount.incrementAndGet();
    if (result <= 1) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "incref on a closed log: " + this);
    }
  }

  public boolean try_incref() {
    return refcount.incrementAndGet() > 1;
  }

  public void decref() {
    if (refcount.decrementAndGet() == 0) {
      close();
    }
  }

  /** returns the current position in the log file */
  public long position() {
    synchronized (this) {
      return fos.size();
    }
  }

  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE) return;
    try {
      synchronized (this) {
        fos.flushBuffer();
      }

      if (syncLevel == UpdateLog.SyncLevel.FSYNC) {
        // Since fsync is outside of synchronized block, we can end up with a partial
        // last record on power failure (which is OK, and does not represent an error...
        // we just need to be aware of it when reading).
        raf.getFD().sync();
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void close() {
    try {
      if (debug) {
        log.debug("Closing tlog" + this);
      }

      synchronized (this) {
        fos.flush();
        fos.close();
      }

      if (deleteOnClose) {
        tlogFile.delete();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
  
  public void forceClose() {
    if (refcount.get() > 0) {
      log.error("Error: Forcing close of " + this);
      refcount.set(0);
      close();
    }
  }

  public String toString() {
    return "tlog{file=" + tlogFile.toString() + " refcount=" + refcount.get() + "}";
  }

  /** Returns a reader that can be used while a log is still in use.
   * Currently only *one* LogReader may be outstanding, and that log may only
   * be used from a single thread. */
  public LogReader getReader(long startingPos) {
    return new LogReader(startingPos);
  }

  /** Returns a single threaded reverse reader */
  public ReverseReader getReverseReader() throws IOException {
    return new ReverseReader();
  }


  public class LogReader {
    ChannelFastInputStream fis;
    private LogCodec codec = new LogCodec();

    public LogReader(long startingPos) {
      incref();
      fis = new ChannelFastInputStream(channel, startingPos);
    }

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException
     */
    public Object next() throws IOException, InterruptedException {
      long pos = fis.position();


      synchronized (TransactionLog.this) {
        if (debug) {
          log.debug("Reading log record.  pos="+pos+" currentSize="+fos.size());
        }

        if (pos >= fos.size()) {
          return null;
        }

        fos.flushBuffer();
      }

      if (pos == 0) {
        readHeader(fis);

        // shouldn't currently happen - header and first record are currently written at the same time
        synchronized (TransactionLog.this) {
          if (fis.position() >= fos.size()) {
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
      decref();
    }

    @Override
    public String toString() {
      synchronized (TransactionLog.this) {
        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + fos.size() + "}";
      }
    }

  }

  public class ReverseReader {
    ChannelFastInputStream fis;
    private LogCodec codec = new LogCodec() {
      @Override
      public SolrInputDocument readSolrInputDocument(FastInputStream dis) throws IOException {
        // Given that the SolrInputDocument is last in an add record, it's OK to just skip
        // reading it completely.
        return null;
      }
    };

    int nextLength;  // length of the next record (the next one closer to the start of the log file)
    long prevPos;    // where we started reading from last time (so prevPos - nextLength == start of next record)

    public ReverseReader() throws IOException {
      incref();

      long sz;
      synchronized (TransactionLog.this) {
        fos.flushBuffer();
        sz = fos.size();
        assert sz == channel.size();
      }

      fis = new ChannelFastInputStream(channel, 0);
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
     * @throws IOException
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
      decref();
    }

    @Override
    public String toString() {
      synchronized (TransactionLog.this) {
        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + fos.size() + "}";
      }
    }


  }

}



class ChannelFastInputStream extends FastInputStream {
  private FileChannel ch;

  public ChannelFastInputStream(FileChannel ch, long chPosition) {
    // super(null, new byte[10],0,0);    // a small buffer size for testing purposes
    super(null);
    this.ch = ch;
    super.readFromStream = chPosition;
  }

  @Override
  public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
    ByteBuffer bb = ByteBuffer.wrap(target, offset, len);
    int ret = ch.read(bb, readFromStream);
    return ret;
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
    ch.close();
  }
  
  @Override
  public String toString() {
    return "readFromStream="+readFromStream +" pos="+pos +" end="+end + " bufferPos="+getBufferPos() + " position="+position() ;
  }
}

