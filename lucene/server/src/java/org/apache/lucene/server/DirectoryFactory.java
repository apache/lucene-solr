package org.apache.lucene.server;

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

import java.io.File;
import java.io.IOException;

//import org.apache.lucene.store.AsyncFSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

/** A factory to open a {@link Directory} from a provided
 *  filesystem path. */
public abstract class DirectoryFactory {

  /** Sole constructor. */
  public DirectoryFactory() {
  }

  /** Open a new {@link Directory} at the specified path. */
  public abstract Directory open(File path) throws IOException;

  /** Returns an instance, using the specified
   *  implementation {FSDirectory, MMapDirectory,
   *  NIOFSDirectory, SimpleFSDirectory or RAMDirectory}. */
  public static DirectoryFactory get(String dirImpl) {
    if (dirImpl.equals("FSDirectory")) {
      return new DirectoryFactory() {
          @Override
          public Directory open(File path) throws IOException {
            return FSDirectory.open(path);
          }
        };
    } else if (dirImpl.equals("MMapDirectory")) {
      return new DirectoryFactory() {
          @Override
          public Directory open(File path) throws IOException {
            return new MMapDirectory(path);
          }
        };
    } else if (dirImpl.equals("NIOFSDirectory")) {
      return new DirectoryFactory() {
          @Override
          public Directory open(File path) throws IOException {
            return new NIOFSDirectory(path);
          }
        };
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      return new DirectoryFactory() {
          @Override
          public Directory open(File path) throws IOException {
            return new SimpleFSDirectory(path);
          }
        };
      /*} else if (dirImpl.equals("AsyncFSDirectory")) {
      return new DirectoryFactory() {
          @Override
          public Directory open(File path) throws IOException {
            return new AsyncFSDirectory(path);
          }
          };*/
    } else if (dirImpl.equals("RAMDirectory")) {
      return new DirectoryFactory() {
          @Override
          public Directory open(File path) throws IOException {
            return new RAMDirectory();
            /*
            final long t0 = System.currentTimeMillis();
            Directory dir =new RAMDirectory(new SimpleFSDirectory(path), IOContext.READ);
            System.out.println((System.currentTimeMillis() - t0) + " msec to load RAMDir; sizeInBytes=" + ((RAMDirectory) dir).sizeInBytes());
            return dir;
            */
          }
      };
    } else {
      // nocommit we should try to load/instantiate the class
      // to allow custom dir impls, here
      throw new IllegalArgumentException("unknown directory implementation \"" + dirImpl + "\"");
    }
  }
}
