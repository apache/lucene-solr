package org.apache.lucene.server;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

// TODO: move to Lucene (eg PSDP could use this)

/** Helper class for write-once save/load of state to a
 *  {@link Directory}, ie foo.0, foo.1, ... */
public abstract class GenFileUtil<T> {

  private final String prefix;
  private final Directory dir;

  private long nextWriteGen;

  /** Sole constructor. */
  protected GenFileUtil(Directory dir, String prefix) {
    this.dir = dir;
    this.prefix = prefix + ".";
  }

  /** Next generation to write. */
  public long getNextWriteGen() {
    return nextWriteGen;
  }

  /** Loads the most recent generation file. */
  protected synchronized T load() throws IOException {
    long genLoaded = -1;
    IOException ioe = null;

    // Holds all <prefix>_N files we've seen, so we can
    // remove stale ones:
    List<String> genFiles = new ArrayList<String>();
    String[] files;
    try {
      files = dir.listAll();
    } catch (IOException ioe2) {
      return null;
    }

    T loaded = null;

    for(String file : dir.listAll()) {
      if (file.startsWith(prefix)) {
        long gen = Long.parseLong(file.substring(prefix.length()));
        if (genLoaded == -1 || gen > genLoaded) {
          genFiles.add(file);
          IndexInput in = dir.openInput(file, IOContext.DEFAULT);
          try {
            loaded = loadOne(in);
          } catch (IOException ioe2) {
            // Save first exception & throw in the end
            if (ioe == null) {
              ioe = ioe2;
            }
          } finally {
            in.close();
          }
          genLoaded = gen;
        }
      }
    }

    if (genLoaded == -1) {
      // Nothing was loaded...
      if (ioe != null) {
        // ... not for lack of trying:
        throw ioe;
      }
    } else { 
      if (genFiles.size() > 1) {
        // Remove any broken / old files:
        String curFileName = prefix + genLoaded;
        for(String file : genFiles) {
          long gen = Long.parseLong(file.substring(prefix.length()));
          if (canDelete(gen) && !curFileName.equals(file)) {
            dir.deleteFile(file);
          }
        }
      }
      nextWriteGen = 1+genLoaded;
    }

    return loaded;
  }

  /** True if this generation is no longer in use; subclass
   *  can override this to implement a "deletion policy". */
  protected boolean canDelete(long gen) {
    return true;
  }

  /** Save the object to the next write generation. */
  public synchronized void save(T o) throws IOException {

    String fileName = prefix + nextWriteGen;
    //System.out.println("write to " + fileName);
    IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT);
    boolean success = false;
    try {
      saveOne(out, o);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
        try {
          dir.deleteFile(fileName);
        } catch (Exception e) {
          // Suppress so we keep throwing original exception
        }
      } else {
        IOUtils.close(out);
      }
    }

    dir.sync(Collections.singletonList(fileName));
    if (nextWriteGen > 0 && canDelete(nextWriteGen-1)) {
      String oldFileName = prefix + (nextWriteGen-1);
      if (dir.fileExists(oldFileName)) {
        dir.deleteFile(oldFileName);
      }
    }

    nextWriteGen++;
  }

  /** Load the object from the provided {@link IndexInput}. */
  protected abstract T loadOne(IndexInput in) throws IOException;

  /** Save the object to the provided {@link IndexOutput}. */
  protected abstract void saveOne(IndexOutput out, T obj) throws IOException;
}
