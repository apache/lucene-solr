package org.apache.solr.update;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class NullUpdateLog extends UpdateLog {
  @Override
  public void init(PluginInfo info) {
  }

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
  }

  @Override
  public void add(AddUpdateCommand cmd) {
  }

  @Override
  public void delete(DeleteUpdateCommand cmd) {
  }

  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) {
  }

  @Override
  public void preCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public void postCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public void preSoftCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public void postSoftCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public Object lookup(BytesRef indexedId) {
    return null;
  }

  @Override
  public void close() {
  }
}

public class FSUpdateLog extends UpdateLog {

  public static String TLOG_NAME="tlog";

  long id = -1;

  private TransactionLog tlog;
  private TransactionLog prevTlog;

  private Map<BytesRef,LogPtr> map = new HashMap<BytesRef, LogPtr>();
  private Map<BytesRef,LogPtr> prevMap;  // used while committing/reopening is happening
  private Map<BytesRef,LogPtr> prevMap2;  // used while committing/reopening is happening
  private TransactionLog prevMapLog;  // the transaction log used to look up entries found in prevMap
  private TransactionLog prevMapLog2;  // the transaction log used to look up entries found in prevMap

  private String[] tlogFiles;
  private File tlogDir;
  private Collection<String> globalStrings;

  private String dataDir;
  private String lastDataDir;

  @Override
  public void init(PluginInfo info) {
    dataDir = (String)info.initArgs.get("dir");
  }

  public void init(UpdateHandler uhandler, SolrCore core) {
    if (dataDir == null || dataDir.length()==0) {
      dataDir = core.getDataDir();
    }

    if (dataDir.equals(lastDataDir)) {
      // on a normal reopen, we currently shouldn't have to do anything
      return;
    }
    lastDataDir = dataDir;
    tlogDir = new File(dataDir, TLOG_NAME);
    tlogDir.mkdirs();
    tlogFiles = getLogList(tlogDir);
    id = getLastLogId() + 1;   // add 1 since we will create a new log for the next update
  }

  static class LogPtr {
    final long pointer;
    public LogPtr(long pointer) {
      this.pointer = pointer;
    }

    public String toString() {
      return "LogPtr(" + pointer + ")";
    }
  }

  public static String[] getLogList(File directory) {
    final String prefix = TLOG_NAME+'.';
    String[] names = directory.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix);
      }
    });
    Arrays.sort(names);
    return names;
  }


  public long getLastLogId() {
    if (id != -1) return id;
    if (tlogFiles.length == 0) return -1;
    String last = tlogFiles[tlogFiles.length-1];
    return Long.parseLong(last.substring(TLOG_NAME.length()+1));
  }


  @Override
  public void add(AddUpdateCommand cmd) {
    synchronized (this) {
      ensureLog();
      long pos = tlog.write(cmd);
      LogPtr ptr = new LogPtr(pos);
      map.put(cmd.getIndexedId(), ptr);
      // System.out.println("TLOG: added id " + cmd.getPrintableId() + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
    }
  }

  @Override
  public void delete(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();

    synchronized (this) {
      ensureLog();
      long pos = tlog.writeDelete(cmd);
      LogPtr ptr = new LogPtr(pos);
      map.put(br, ptr);
      // System.out.println("TLOG: added delete for id " + cmd.id + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
    }
  }

  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) {
    synchronized (this) {
      ensureLog();
      // TODO: how to support realtime-get, optimistic concurrency, or anything else in this case?
      // Maybe we shouldn't?
      // realtime-get could just do a reopen of the searcher
      // optimistic concurrency? Maybe we shouldn't support deleteByQuery w/ optimistic concurrency
      long pos = tlog.writeDeleteByQuery(cmd);
      LogPtr ptr = new LogPtr(pos);
      // System.out.println("TLOG: added deleteByQuery " + cmd.query + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
    }
  }


  private void newMap() {
    prevMap2 = prevMap;
    prevMapLog2 = prevMapLog;

    prevMap = map;
    prevMapLog = tlog;

    map = new HashMap<BytesRef, LogPtr>();
  }

  private void clearOldMaps() {
    prevMap = null;
    prevMap2 = null;
  }

  @Override
  public void preCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      // since we're changing the log, we must change the map.
      newMap();

      // since document additions can happen concurrently with commit, create
      // a new transaction log first so that we know the old one is definitely
      // in the index.
      prevTlog = tlog;
      tlog = null;
      id++;

      if (prevTlog != null) {
        globalStrings = prevTlog.getGlobalStrings();
      }
    }
  }

  @Override
  public void postCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      if (prevTlog != null) {
        prevTlog.decref();
        prevTlog = null;
      }
    }
  }

  @Override
  public void preSoftCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      if (!cmd.softCommit) return;  // already handled this at the start of the hard commit
      newMap();

      // start adding documents to a new map since we won't know if
      // any added documents will make it into this commit or not.
      // But we do know that any updates already added will definitely
      // show up in the latest reader after the commit succeeds.
      map = new HashMap<BytesRef, LogPtr>();
      // System.out.println("TLOG: preSoftCommit: prevMap="+ System.identityHashCode(prevMap) + " new map=" + System.identityHashCode(map));
    }
  }

  @Override
  public void postSoftCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      // We can clear out all old maps now that a new searcher has been opened.
      // This currently only works since DUH2 synchronizes around preCommit to avoid
      // it being called in the middle of a preSoftCommit, postSoftCommit sequence.
      // If this DUH2 synchronization were to be removed, preSoftCommit should
      // record what old maps were created and only remove those.
      clearOldMaps();
      // System.out.println("TLOG: postSoftCommit: disposing of prevMap="+ System.identityHashCode(prevMap));
    }
  }

  @Override
  public Object lookup(BytesRef indexedId) {
    LogPtr entry;
    TransactionLog lookupLog;

    synchronized (this) {
      entry = map.get(indexedId);
      lookupLog = tlog;  // something found in "map" will always be in "tlog"
      // System.out.println("TLOG: lookup: for id " + indexedId.utf8ToString() + " in map " +  System.identityHashCode(map) + " got " + entry + " lookupLog=" + lookupLog);
      if (entry == null && prevMap != null) {
        entry = prevMap.get(indexedId);
        // something found in prevMap will always be found in preMapLog (which could be tlog or prevTlog)
        lookupLog = prevMapLog;
        // System.out.println("TLOG: lookup: for id " + indexedId.utf8ToString() + " in prevMap " +  System.identityHashCode(prevMap) + " got " + entry + " lookupLog="+lookupLog);
      }
      if (entry == null && prevMap2 != null) {
        entry = prevMap2.get(indexedId);
        // something found in prevMap2 will always be found in preMapLog2 (which could be tlog or prevTlog)
        lookupLog = prevMapLog2;
        // System.out.println("TLOG: lookup: for id " + indexedId.utf8ToString() + " in prevMap2 " +  System.identityHashCode(prevMap) + " got " + entry + " lookupLog="+lookupLog);
      }

      if (entry == null) {
        return null;
      }
      lookupLog.incref();
    }

    try {
      // now do the lookup outside of the sync block for concurrency
      return lookupLog.lookup(entry.pointer);
    } finally {
      lookupLog.decref();
    }

  }


  private void ensureLog() {
    if (tlog == null) {
      String newLogName = String.format("%s.%019d", TLOG_NAME, id);
      tlog = new TransactionLog(new File(tlogDir, newLogName), globalStrings);
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      if (prevTlog != null) {
        prevTlog.decref();
      }
      if (tlog != null) {
        tlog.decref();
      }
    }
  }


}


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
class TransactionLog {

  long id;
  File tlogFile;
  RandomAccessFile raf;
  FileChannel channel;
  OutputStream os;
  FastOutputStream fos;
  InputStream is;
  long start;

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
      long pos = start + fos.size();   // if we had flushed, this should be equal to channel.position()
      codec.init(fos);
      codec.writeVal(o);
      return pos;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings) {
    try {
      this.tlogFile = tlogFile;
      raf = new RandomAccessFile(this.tlogFile, "rw");
      start = raf.length();
      channel = raf.getChannel();
      os = Channels.newOutputStream(channel);
      fos = FastOutputStream.wrap(os);
      addGlobalStrings(globalStrings);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
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
    NamedList header = new NamedList<Object>();
    header.add("SOLR_TLOG",1); // a magic string + version number?
    header.add("strings",globalStringList);
    codec.marshal(header, fos);
  }


  public long write(AddUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
      try {
        long pos = start + fos.size();   // if we had flushed, this should be equal to channel.position()
        SolrInputDocument sdoc = cmd.getSolrInputDocument();

        if (pos == 0) { // TODO: needs to be changed if we start writing a header first
          addGlobalStrings(sdoc.getFieldNames());
          pos = start + fos.size();
        }

        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.ADD);  // should just take one byte
        codec.writeLong(0);  // the version... should also just be one byte if 0
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
        long pos = start + fos.size();   // if we had flushed, this should be equal to channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = start + fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.DELETE);  // should just take one byte
        codec.writeLong(0);  // the version... should also just be one byte if 0
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
        long pos = start + fos.size();   // if we had flushed, this should be equal to channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = start + fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.DELETE_BY_QUERY);  // should just take one byte
        codec.writeLong(0);  // the version... should also just be one byte if 0
        codec.writeStr(cmd.query);
        // fos.flushBuffer();  // flush later
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  /* This method is thread safe */
  public Object lookup(long pos) {
    try {
      // make sure any unflushed buffer has been flushed
      synchronized (fos) {
        // TODO: optimize this by keeping track of what we have flushed up to
        fos.flushBuffer();
      }

      ChannelFastInputStream fis = new ChannelFastInputStream(channel, pos);
      LogCodec codec = new LogCodec();
      return codec.readVal(fis);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void incref() {
    refcount.incrementAndGet();
  }

  public void decref() {
    if (refcount.decrementAndGet() == 0) {
      close();
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

}


class ChannelFastInputStream extends FastInputStream {
  FileChannel ch;
  long chPosition;

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

