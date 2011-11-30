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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.zookeeper.Transaction;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
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

  public final static String END_MESSAGE="SOLR_TLOG_END";

  long id;
  File tlogFile;
  RandomAccessFile raf;
  FileChannel channel;
  OutputStream os;
  FastOutputStream fos;    // all accesses to this stream should be synchronized on "this"

  volatile boolean deleteOnClose = true;  // we can delete old tlogs since they are currently only used for real-time-get (and in the future, recovery)

  AtomicInteger refcount = new AtomicInteger(1);
  Map<String,Integer> globalStringMap = new HashMap<String, Integer>();
  List<String> globalStringList = new ArrayList<String>();

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

  TransactionLog(File tlogFile, Collection<String> globalStrings) {
    this(tlogFile, globalStrings, false);
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    try {
      this.tlogFile = tlogFile;
      raf = new RandomAccessFile(this.tlogFile, "rw");
      long start = raf.length();
      channel = raf.getChannel();
      os = Channels.newOutputStream(channel);
      fos = FastOutputStream.wrap(os);

      if (openExisting) {
        if (start > 0) {
          readHeader(null);
          fos.setWritten(start);    // reflect that we aren't starting at the beginning
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

  private void readHeader(FastInputStream fis) throws IOException {
    // read existing header
    fis = fis != null ? fis : new ChannelFastInputStream(channel, 0);
    LogCodec codec = new LogCodec();
    Map header = (Map)codec.unmarshal(fis);
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
    synchronized (fos) {
      return new ArrayList<String>(globalStringList);
    }
  }

  private void writeLogHeader(LogCodec codec) throws IOException {
    Map header = new LinkedHashMap<String,Object>();
    header.put("SOLR_TLOG",1); // a magic string + version number
    header.put("strings",globalStringList);
    codec.marshal(header, fos);
  }


  public long write(AddUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
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
        // fos.flushBuffer();  // flush later


        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
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
        // fos.flushBuffer();  // flush later
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
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
        // fos.flushBuffer();  // flush later
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }


  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
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
        codec.writeStr(END_MESSAGE);  // ensure these bytes are the last in the file
        // fos.flushBuffer();  // flush later
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
      synchronized (fos) {
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

  public void decref() {
    if (refcount.decrementAndGet() == 0) {
      close();
    }
  }

  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE) return;
    try {
      synchronized (fos) {
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
      fos.flush();
      fos.close();
      if (deleteOnClose) {
        tlogFile.delete();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public String toString() {
    return tlogFile.toString();
  }

  /** Returns a reader that can be used while a log is still in use.
   * Currently only *one* log may be outstanding, and that log may only
   * be used from a single thread. */
  public LogReader getReader() {
    return new LogReader();
  }



  public class LogReader {
    ChannelFastInputStream fis = new ChannelFastInputStream(channel, 0);
    private LogCodec codec = new LogCodec();

    public LogReader() {
      incref();
    }

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException
     */
    public Object next() throws IOException, InterruptedException {
      long pos = fis.position();

      synchronized (TransactionLog.this) {
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
        }
      }

      return codec.readVal(fis);
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
  FileChannel ch;
  private long chPosition;

  public ChannelFastInputStream(FileChannel ch, long chPosition) {
    super(null);
    this.ch = ch;
    this.chPosition = chPosition;
  }

  @Override
  public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
    ByteBuffer bb = ByteBuffer.wrap(target, offset, len);
    int ret = ch.read(bb, chPosition);
    if (ret >= 0) {
      chPosition += ret;
    }
    return ret;
  }

  @Override
  public void close() throws IOException {
    ch.close();
  }
}

