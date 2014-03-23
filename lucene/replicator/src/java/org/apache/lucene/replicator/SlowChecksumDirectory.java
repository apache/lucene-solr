package org.apache.lucene.replicator;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.TreeLogger;

/** Stupid wrapper to inefficietly compute CRC32 checksum of
 *  every written file after it's closed. */

// nocommit this is silly wrapper until we integrate
// checkums at a lower lever / written to each file

public class SlowChecksumDirectory extends FilterDirectory {

  private final Checksums checksums;

  // Closed but not yet sync'd:
  private final Map<String,Long> pendingChecksums = new ConcurrentHashMap<String,Long>();

  public SlowChecksumDirectory(int id, Directory in) throws IOException {
    super(in);
    checksums = new Checksums(id, in);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    IndexOutput other = in.createOutput(name, context);
    return new SlowChecksumIndexOutput(this, name, other);
  }

  /** Stupid: this is called when an output is closed, and
   *  we go and re-read it to compute the checksum. */
  void outputClosed(String name) throws IOException {
    IndexInput input = in.openInput(name, IOContext.READONCE);
    try {
      byte[] bytes = new byte[4096];
      long bytesLeft = in.fileLength(name);

      Checksum checksum = new CRC32();
      while (bytesLeft > 0) {
        int chunk = (int) Math.min(bytesLeft, bytes.length);
        input.readBytes(bytes, 0, chunk);
        checksum.update(bytes, 0, chunk);
        bytesLeft -= chunk;
      }

      long value = checksum.getValue();
      System.out.println(Thread.currentThread().getName() + " id=" + checksums.id + " " + name + ": record pending checksum=" + value);
      pendingChecksums.put(name, value);
    } finally {
      input.close();
    }
  }

  public Long getChecksum(String name) {
    Long v = pendingChecksums.get(name);
    if (v != null) {
      return v;
    }
    v = checksums.get(name);
    if (v != null) {
      return v;
    }
    return null;
  }

  public void sync(Collection<String> names) throws IOException {
    System.out.println(Thread.currentThread().getName() + " id=" + checksums.id + " sync " + names);
    in.sync(names);
    for(String name : names) {
      Long v = pendingChecksums.get(name);
      if (v == null) {
        assert checksums.get(name) != null: "name=" + name + " has no checksum";
      } else {
        checksums.add(name, v.longValue(), false);
      }
    }
    checksums.save();

    for(String name : names) {
      pendingChecksums.remove(name);
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    System.out.println(Thread.currentThread().getName() + " id=" + checksums.id + " " + name + " now delete");
    in.deleteFile(name);
    pendingChecksums.remove(name);
    checksums.remove(name);
  }

  @Override
  public void close() throws IOException {
    try {
      checksums.save();
    } finally {
      in.close();
    }
  }

  static class Checksums {

    // nocommit we need to remove old checksum file when
    // writing new one:

    private static final String FILE_NAME_PREFIX = "checksums";
    private static final String CHECKSUM_CODEC = "checksum";
    private static final int CHECKSUM_VERSION_START = 0;
    private static final int CHECKSUM_VERSION_CURRENT = CHECKSUM_VERSION_START;

    // nocommit need to sometimes prune this map

    final Map<String,Long> checksums = new HashMap<String,Long>();
    private final Directory dir;
    final int id;

    private long nextWriteGen;

    // nocommit need to test crashing after writing checksum
    // & before committing

    public Checksums(int id, Directory dir) throws IOException {
      this.id = id;
      this.dir = dir;
      long maxGen = -1;
      for (String fileName : dir.listAll()) {
        if (fileName.startsWith(FILE_NAME_PREFIX)) {
          long gen = Long.parseLong(fileName.substring(1+FILE_NAME_PREFIX.length()),
                                    Character.MAX_RADIX);
          if (gen > maxGen) {
            maxGen = gen;
          }
        }
      }

      while (maxGen > -1) {
        IndexInput in = dir.openInput(genToFileName(maxGen), IOContext.DEFAULT);
        try {
          int version = CodecUtil.checkHeader(in, CHECKSUM_CODEC, CHECKSUM_VERSION_START, CHECKSUM_VERSION_START);
          if (version != CHECKSUM_VERSION_START) {
            throw new CorruptIndexException("wrong checksum version");
          }
          int count = in.readVInt();
          for(int i=0;i<count;i++) {
            String name = in.readString();
            long checksum = in.readLong();
            checksums.put(name, checksum);
          }
          nextWriteGen = maxGen+1;
          System.out.println(Thread.currentThread().getName() + ": id=" + id + " " + genToFileName(maxGen) + " loaded checksums");
          break;
        } catch (IOException ioe) {
          // This file was truncated, probably due to
          // crashing w/o syncing:
          maxGen--;
        } finally {
          in.close();
        }
      }
    }

    public synchronized void remove(String name) {
      checksums.remove(name);
    }

    private static String genToFileName(long gen) {
      return FILE_NAME_PREFIX + "_" + Long.toString(gen, Character.MAX_RADIX);
    }
    
    public synchronized void add(String fileName, long checksum, boolean mustMatch) {
      Long oldChecksum = checksums.put(fileName, checksum);
      if (oldChecksum == null) {
        System.out.println(Thread.currentThread().getName() + ": id=" + id + " " + fileName + " record checksum=" + checksum);
      } else if (oldChecksum.longValue() != checksum) {
        System.out.println(Thread.currentThread().getName() + ": id=" + id + " " + fileName + " record new checksum=" + checksum + " replaces old checksum=" + oldChecksum);
      }
      // cannot assert this: in the "master rolled back"
      // case, this can easily happen:
      // assert mustMatch == false || oldChecksum == null || oldChecksum.longValue() == checksum: "fileName=" + fileName + " oldChecksum=" + oldChecksum + " newChecksum=" + checksum;
    }

    public synchronized Long get(String name) {
      return checksums.get(name);
    }

    public synchronized void save() throws IOException {
      String fileName = genToFileName(nextWriteGen++);
      System.out.println(Thread.currentThread().getName() + " id=" + id + " save checksums to file \"" + fileName + "\"; files=" + checksums.keySet());
      IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT);
      try {
        CodecUtil.writeHeader(out, CHECKSUM_CODEC, CHECKSUM_VERSION_CURRENT);
        out.writeVInt(checksums.size());
        for(Map.Entry<String,Long> ent : checksums.entrySet()) {
          out.writeString(ent.getKey());
          out.writeLong(ent.getValue());
        }
      } finally {
        out.close();
      }

      dir.sync(Collections.singletonList(fileName));

      if (nextWriteGen > 1) {
        String oldFileName = genToFileName(nextWriteGen-2);
        try {
          dir.deleteFile(oldFileName);
        } catch (IOException ioe) {
        }
      }
    }
  }
}
