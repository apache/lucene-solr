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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/** Common base class for {@link PrimaryNode} and {@link ReplicaNode}.
 *
 * @lucene.experimental */

public abstract class Node implements Closeable {

  public static boolean VERBOSE_FILES = true;
  public static boolean VERBOSE_CONNECTIONS = false;

  // Keys we store into IndexWriter's commit user data:

  /** Key to store the primary gen in the commit data, which increments every time we promote a new primary, so replicas can detect when the
   *  primary they were talking to is changed */
  public static String PRIMARY_GEN_KEY = "__primaryGen";

  /** Key to store the version in the commit data, which increments every time we open a new NRT reader */
  public static String VERSION_KEY = "__version";

  /** Compact ordinal for this node */
  protected final int id;

  protected final Directory dir;

  protected final SearcherFactory searcherFactory;
  
  // Tracks NRT readers, opened from IW (primary) or opened from replicated SegmentInfos pulled across the wire (replica):
  protected ReferenceManager<IndexSearcher> mgr;

  /** Startup time of original test, carefully propogated to all nodes to produce consistent "seconds since start time" in messages */
  public static long globalStartNS;

  /** When this node was started */
  public static final long localStartNS = System.nanoTime();

  /** For debug logging */
  protected final PrintStream printStream;

  // public static final long globalStartNS;

  // For debugging:
  volatile String state = "idle";

  /** File metadata for last sync that succeeded; we use this as a cache */
  protected volatile Map<String,FileMetaData> lastFileMetaData;

  public Node(int id, Directory dir, SearcherFactory searcherFactory, PrintStream printStream) {
    this.id = id;
    this.dir = dir;
    this.searcherFactory = searcherFactory;
    this.printStream = printStream;
  }

  /** Returns the {@link ReferenceManager} to use for acquiring and releasing searchers */
  public ReferenceManager<IndexSearcher> getSearcherManager() {
    return mgr;
  }

  /** Returns the {@link Directory} this node is writing to */
  public Directory getDirectory() {
    return dir;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(id=" + id + ")";
  }

  public abstract void commit() throws IOException;

  public static void nodeMessage(PrintStream printStream, String message) {
    if (printStream != null) {
      long now = System.nanoTime();
      printStream.println(String.format(Locale.ROOT,
                                        "%5.3fs %5.1fs:           [%11s] %s",
                                        (now-globalStartNS)/1000000000.,
                                        (now-localStartNS)/1000000000.,
                                        Thread.currentThread().getName(),
                                        message));
    }
  }

  public static void nodeMessage(PrintStream printStream, int id, String message) {
    if (printStream != null) {
      long now = System.nanoTime();
      printStream.println(String.format(Locale.ROOT,
                                       "%5.3fs %5.1fs:         N%d [%11s] %s",
                                       (now-globalStartNS)/1000000000.,
                                       (now-localStartNS)/1000000000.,
                                       id,
                                       Thread.currentThread().getName(),
                                       message));
    }
  }

  public void message(String message) {
    if (printStream != null) {
      long now = System.nanoTime();
      printStream.println(String.format(Locale.ROOT,
                                       "%5.3fs %5.1fs: %7s %2s [%11s] %s",
                                       (now-globalStartNS)/1000000000.,
                                       (now-localStartNS)/1000000000.,
                                       state, name(),
                                       Thread.currentThread().getName(), message));
    }
  }

  public String name() {
    char mode = this instanceof PrimaryNode ? 'P' : 'R';
    return mode + Integer.toString(id);
  }

  public abstract boolean isClosed();

  public long getCurrentSearchingVersion() throws IOException {
    IndexSearcher searcher = mgr.acquire();
    try {
      return ((DirectoryReader) searcher.getIndexReader()).getVersion();
    } finally {
      mgr.release(searcher);
    }
  }

  public static String bytesToString(long bytes) {
    if (bytes < 1024) {
      return bytes + " b";
    } else if (bytes < 1024 * 1024) {
      return String.format(Locale.ROOT, "%.1f KB", bytes/1024.);
    } else if (bytes < 1024 * 1024 * 1024) {
      return String.format(Locale.ROOT, "%.1f MB", bytes/1024./1024.);
    } else {
      return String.format(Locale.ROOT, "%.1f GB", bytes/1024./1024./1024.);
    }
  }

  /** Opens the specified file, reads its identifying information, including file length, full index header (includes the unique segment
   *  ID) and the full footer (includes checksum), and returns the resulting {@link FileMetaData}.
   *
   *  <p>This returns null, logging a message, if there are any problems (the file does not exist, is corrupt, truncated, etc.).</p> */
  public FileMetaData readLocalFileMetaData(String fileName) throws IOException {

    Map<String,FileMetaData> cache = lastFileMetaData;
    FileMetaData result;
    if (cache != null) {
      // We may already have this file cached from the last NRT point:
      result = cache.get(fileName);
    } else {
      result = null;
    }

    if (result == null) {
      // Pull from the filesystem
      long checksum;
      long length;
      byte[] header;
      byte[] footer;
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
          try {
            length = in.length();
            header = CodecUtil.readIndexHeader(in);
            footer = CodecUtil.readFooter(in);
            checksum = CodecUtil.retrieveChecksum(in);
          } catch (EOFException | CorruptIndexException cie) {
            // File exists but is busted: we must copy it.  This happens when node had crashed, corrupting an un-fsync'd file.  On init we try
            // to delete such unreferenced files, but virus checker can block that, leaving this bad file.
            if (VERBOSE_FILES) {
              message("file " + fileName + ": will copy [existing file is corrupt]");
            }
            return null;
          }
          if (VERBOSE_FILES) {
            message("file " + fileName + " has length=" + bytesToString(length));
          }
        } catch (FileNotFoundException | NoSuchFileException e) {
        if (VERBOSE_FILES) {
          message("file " + fileName + ": will copy [file does not exist]");
        }
        return null;
      }

      // NOTE: checksum is redundant w/ footer, but we break it out separately because when the bits cross the wire we need direct access to
      // checksum when copying to catch bit flips:
      result = new FileMetaData(header, footer, length, checksum);
    }

    return result;
  }
}
