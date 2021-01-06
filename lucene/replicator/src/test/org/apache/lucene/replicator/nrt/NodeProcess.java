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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.document.Document;

/**
 * Parent JVM hold this "wrapper" to refer to each child JVM. This is roughly equivalent e.g. to a
 * client-side "sugar" API.
 */
class NodeProcess implements Closeable {
  final Process p;

  // Port sub-process is listening on
  final int tcpPort;

  final int id;

  final Thread pumper;

  // Acquired when searching or indexing wants to use this node:
  final ReentrantLock lock;

  final boolean isPrimary;

  // Version in the commit point we opened on init:
  final long initCommitVersion;

  // SegmentInfos.version, which can be higher than the initCommitVersion
  final long initInfosVersion;

  volatile boolean isOpen = true;

  final AtomicBoolean nodeIsClosing;

  public NodeProcess(
      Process p,
      int id,
      int tcpPort,
      Thread pumper,
      boolean isPrimary,
      long initCommitVersion,
      long initInfosVersion,
      AtomicBoolean nodeIsClosing) {
    this.p = p;
    this.id = id;
    this.tcpPort = tcpPort;
    this.pumper = pumper;
    this.isPrimary = isPrimary;
    this.initCommitVersion = initCommitVersion;
    this.initInfosVersion = initInfosVersion;
    this.nodeIsClosing = nodeIsClosing;
    assert initInfosVersion >= initCommitVersion
        : "initInfosVersion=" + initInfosVersion + " initCommitVersion=" + initCommitVersion;
    lock = new ReentrantLock();
  }

  @Override
  public String toString() {
    if (isPrimary) {
      return "P" + id + " tcpPort=" + tcpPort;
    } else {
      return "R" + id + " tcpPort=" + tcpPort;
    }
  }

  public synchronized void crash() {
    if (isOpen) {
      isOpen = false;
      p.destroy();
      try {
        p.waitFor();
        pumper.join();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
    }
  }

  public boolean commit() throws IOException {
    try (Connection c = new Connection(tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_COMMIT);
      c.flush();
      c.s.shutdownOutput();
      if (c.in.readByte() != 1) {
        throw new RuntimeException("commit failed");
      }
      return true;
    }
  }

  public void commitAsync() throws IOException {
    try (Connection c = new Connection(tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_COMMIT);
      c.flush();
    }
  }

  public long getSearchingVersion() throws IOException {
    try (Connection c = new Connection(tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_GET_SEARCHING_VERSION);
      c.flush();
      c.s.shutdownOutput();
      return c.in.readVLong();
    }
  }

  /**
   * Ask the primary node process to flush. We send it all currently up replicas so it can notify
   * them about the new NRT point. Returns the newly flushed version, or a negative (current)
   * version if there were no changes.
   */
  public synchronized long flush(int atLeastMarkerCount) throws IOException {
    assert isPrimary;
    try (Connection c = new Connection(tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_FLUSH);
      c.out.writeVInt(atLeastMarkerCount);
      c.flush();
      c.s.shutdownOutput();
      return c.in.readLong();
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  public synchronized boolean shutdown() {
    lock.lock();
    try {
      // System.out.println("PARENT: now shutdown node=" + id + " isOpen=" + isOpen);
      if (isOpen) {
        // Ask the child process to shutdown gracefully:
        isOpen = false;
        // System.out.println("PARENT: send CMD_CLOSE to node=" + id);
        try (Connection c = new Connection(tcpPort)) {
          c.out.writeByte(SimplePrimaryNode.CMD_CLOSE);
          c.flush();
          if (c.in.readByte() != 1) {
            throw new RuntimeException("shutdown failed");
          }
        } catch (Throwable t) {
          System.out.println("top: shutdown failed; ignoring");
          t.printStackTrace(System.out);
        }
        try {
          p.waitFor();
          pumper.join();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }

      return true;
    } finally {
      lock.unlock();
    }
  }

  public void newNRTPoint(long version, long primaryGen, int primaryTCPPort) throws IOException {
    try (Connection c = new Connection(tcpPort)) {
      c.out.writeByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
      c.out.writeVLong(version);
      c.out.writeVLong(primaryGen);
      c.out.writeInt(primaryTCPPort);
      c.flush();
    }
  }

  public void addOrUpdateDocument(Connection c, Document doc, boolean isUpdate) throws IOException {
    if (isPrimary == false) {
      throw new IllegalStateException("only primary can index");
    }
    int fieldCount = 0;

    String title = doc.get("title");
    if (title != null) {
      fieldCount++;
    }

    String docid = doc.get("docid");
    assert docid != null;
    fieldCount++;

    String body = doc.get("body");
    if (body != null) {
      fieldCount++;
    }

    String marker = doc.get("marker");
    if (marker != null) {
      fieldCount++;
    }

    c.out.writeByte(isUpdate ? SimplePrimaryNode.CMD_UPDATE_DOC : SimplePrimaryNode.CMD_ADD_DOC);
    c.out.writeVInt(fieldCount);
    c.out.writeString("docid");
    c.out.writeString(docid);
    if (title != null) {
      c.out.writeString("title");
      c.out.writeString(title);
    }
    if (body != null) {
      c.out.writeString("body");
      c.out.writeString(body);
    }
    if (marker != null) {
      c.out.writeString("marker");
      c.out.writeString(marker);
    }
    c.flush();
    c.in.readByte();
  }

  public void deleteDocument(Connection c, String docid) throws IOException {
    if (isPrimary == false) {
      throw new IllegalStateException("only primary can delete documents");
    }
    c.out.writeByte(SimplePrimaryNode.CMD_DELETE_DOC);
    c.out.writeString(docid);
    c.flush();
    c.in.readByte();
  }

  public void deleteAllDocuments(Connection c) throws IOException {
    if (isPrimary == false) {
      throw new IllegalStateException("only primary can delete documents");
    }
    c.out.writeByte(SimplePrimaryNode.CMD_DELETE_ALL_DOCS);
    c.flush();
    c.in.readByte();
  }

  public void forceMerge(Connection c) throws IOException {
    if (isPrimary == false) {
      throw new IllegalStateException("only primary can force merge");
    }
    c.out.writeByte(SimplePrimaryNode.CMD_FORCE_MERGE);
    c.flush();
    c.in.readByte();
  }
}
