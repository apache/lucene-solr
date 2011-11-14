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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.JavaBinCodec;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Single threaded transaction log reader */
public class TransactionLogReader {
  private static byte[] END_MESSAGE_BYTES;
  static {
    try {
      END_MESSAGE_BYTES = TransactionLog.END_MESSAGE.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      // impossible
    }
  }

  private File tlogFile;
  private RandomAccessFile raf;
  private FileChannel channel;
  private FastInputStream fis;
  private boolean completed;
  private LogCodec codec;

  private Map header;
  private List<String> globalStringList;


  public class LogCodec extends JavaBinCodec {
    public LogCodec() {
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


  public TransactionLogReader(File tlogFile) {
    try {
      this.tlogFile = tlogFile;
      raf = new RandomAccessFile(tlogFile,"r");
      byte[] end = new byte[END_MESSAGE_BYTES.length];
      long size = raf.length();
      completed = false;
      if (size >= end.length) {
        raf.seek(size - end.length);
        raf.readFully(end);
        completed = Arrays.equals(end, END_MESSAGE_BYTES);
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /** did the file end with END_MESSAGE_BYTES, implying that everything here was committed? */
  public boolean completed() {
    return completed;
  }

  public Map readHeader() {
    if (header != null) return header;
    try {
      raf.seek(0);
      channel = raf.getChannel();
      InputStream is = Channels.newInputStream(channel);
      fis = new FastInputStream(is);
      codec = new LogCodec();
      header = (Map)codec.unmarshal(fis);

      // needed to read other records
      globalStringList = (List<String>)header.get("strings");

      return header;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Corrupt transaction log", e);
    }
  }

  public Object readNext() {
    try {
      readHeader();
      if (fis.peek() == -1) return null;   // EOF
      return codec.readVal(fis);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Corrupt transaction log", e);
    }
  }

  public void close() {
    try {
      raf.close();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Corrupt transaction log", e);
    }
  }

  public void delete() {
    tlogFile.delete();
  }

  @Override
  public String toString() {
    return "TransactionLogReader{"+"file="+tlogFile+"}";
  }
}
