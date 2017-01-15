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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.RAMOutputStream;

/** This is a stupid yet functional transaction log: it never fsync's, never prunes, it's over-synchronized, it hard-wires id field name to "docid", can
 *  only handle specific docs/fields used by this test, etc.  It's just barely enough to show how a translog could work on top of NRT
 *  replication to guarantee no data loss when nodes crash */

class SimpleTransLog implements Closeable {

  final FileChannel channel;
  final RAMOutputStream buffer = new RAMOutputStream();
  final byte[] intBuffer = new byte[4];
  final ByteBuffer intByteBuffer = ByteBuffer.wrap(intBuffer);

  private final static byte OP_ADD_DOCUMENT = (byte) 0;
  private final static byte OP_UPDATE_DOCUMENT = (byte) 1;
  private final static byte OP_DELETE_DOCUMENTS = (byte) 2;

  public SimpleTransLog(Path path) throws IOException {
    channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
  }

  public synchronized long getNextLocation() throws IOException {
    return channel.position();
  }

  /** Appends an addDocument op */
  public synchronized long addDocument(String id, Document doc) throws IOException {
    assert buffer.getFilePointer() == 0;
    buffer.writeByte(OP_ADD_DOCUMENT);
    encode(id, doc);
    return flushBuffer();
  }

  /** Appends an updateDocument op */
  public synchronized long updateDocument(String id, Document doc) throws IOException {
    assert buffer.getFilePointer() == 0;
    buffer.writeByte(OP_UPDATE_DOCUMENT);
    encode(id, doc);
    return flushBuffer();
  }

  /** Appends a deleteDocuments op */
  public synchronized long deleteDocuments(String id) throws IOException {
    assert buffer.getFilePointer() == 0;
    buffer.writeByte(OP_DELETE_DOCUMENTS);
    buffer.writeString(id);
    return flushBuffer();
  }

  /** Writes buffer to the file and returns the start position. */
  private synchronized long flushBuffer() throws IOException {
    long pos = channel.position();
    int len = (int) buffer.getFilePointer();
    byte[] bytes = new byte[len];
    buffer.writeTo(bytes, 0);
    buffer.reset();

    intBuffer[0] = (byte) (len >> 24);
    intBuffer[1] = (byte) (len >> 16);
    intBuffer[2] = (byte) (len >> 8);
    intBuffer[3] = (byte) len;
    intByteBuffer.limit(4);
    intByteBuffer.position(0);

    writeBytesToChannel(intByteBuffer);
    writeBytesToChannel(ByteBuffer.wrap(bytes));

    return pos;
  }

  private void writeBytesToChannel(ByteBuffer src) throws IOException {
    int left = src.limit();
    while (left != 0) {
      left -= channel.write(src);
    }
  }

  private void readBytesFromChannel(long pos, ByteBuffer dest) throws IOException {
    int left = dest.limit() - dest.position();
    long end = pos + left;
    while (pos < end) {
      int inc = channel.read(dest, pos);
      if (inc < 0) {
        throw new EOFException();
      }
      pos += inc;
    }
  }

  /** Replays ops between start and end location against the provided writer.  Can run concurrently with ongoing operations. */
  public void replay(NodeProcess primary, long start, long end) throws IOException {
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      byte[] intBuffer = new byte[4];
      ByteBuffer intByteBuffer = ByteBuffer.wrap(intBuffer);
      ByteArrayDataInput in = new ByteArrayDataInput();

      long pos = start;
      while (pos < end) {
        intByteBuffer.position(0);
        intByteBuffer.limit(4);
        readBytesFromChannel(pos, intByteBuffer);
        pos += 4;
        int len = ((intBuffer[0] & 0xff) << 24) |
          (intBuffer[1] & 0xff) << 16 |
          (intBuffer[2] & 0xff) << 8 |
          (intBuffer[3] & 0xff);

        byte[] bytes = new byte[len];
        readBytesFromChannel(pos, ByteBuffer.wrap(bytes));
        pos += len;

        in.reset(bytes);
        
        byte op = in.readByte();
        //System.out.println("xlog: replay op=" + op);
        switch (op) {
        case 0:
          // We replay add as update:
          replayAddDocument(c, primary, in);
          break;

        case 1:
          // We replay add as update:
          replayAddDocument(c, primary, in);
          break;

        case 2:
          replayDeleteDocuments(c, primary, in);
          break;

        default:
          throw new CorruptIndexException("invalid operation " + op, in);
        }
      }
      assert pos == end;
      //System.out.println("xlog: done replay");
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING_DONE);
      c.flush();
      //System.out.println("xlog: done flush");
      c.in.readByte();
      //System.out.println("xlog: done readByte");
    }
  }

  private void replayAddDocument(Connection c, NodeProcess primary, DataInput in) throws IOException {
    String id = in.readString();

    Document doc = new Document();
    doc.add(new StringField("docid", id, Field.Store.YES));

    String title = readNullableString(in);
    if (title != null) {
      doc.add(new StringField("title", title, Field.Store.NO));
      doc.add(new TextField("titleTokenized", title, Field.Store.NO));
    }
    String body = readNullableString(in);
    if (body != null) {
      doc.add(new TextField("body", body, Field.Store.NO));
    }
    String marker = readNullableString(in);
    if (marker != null) {
      //TestStressNRTReplication.message("xlog: replay marker=" + id);
      doc.add(new StringField("marker", marker, Field.Store.YES));
    }

    // For both add and update originally, we use updateDocument to replay,
    // because the doc could in fact already be in the index:
    // nocomit what if this fails?
    primary.addOrUpdateDocument(c, doc, false);
  }


  private void replayDeleteDocuments(Connection c, NodeProcess primary, DataInput in) throws IOException {
    String id = in.readString();
    // nocomit what if this fails?
    primary.deleteDocument(c, id);
  }

  /** Encodes doc into buffer.  NOTE: this is NOT general purpose!  It only handles the fields used in this test! */
  private synchronized void encode(String id, Document doc) throws IOException {
    assert id.equals(doc.get("docid")): "id=" + id + " vs docid=" + doc.get("docid");
    buffer.writeString(id);
    writeNullableString(doc.get("title"));
    writeNullableString(doc.get("body"));
    writeNullableString(doc.get("marker"));
  }

  private synchronized void writeNullableString(String s) throws IOException {
    if (s == null) {
      buffer.writeByte((byte) 0);
    } else {
      buffer.writeByte((byte) 1);
      buffer.writeString(s);
    }
  }

  private String readNullableString(DataInput in) throws IOException {
    byte b = in.readByte();
    if (b == 0) {
      return null;
    } else if (b == 1) {
      return in.readString();
    } else {
      throw new CorruptIndexException("invalid string lead byte " + b, in);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    channel.close();
  }
}
