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
package org.apache.lucene.mockfile;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

/** 
 * FileSystem that (imperfectly) acts like windows. 
 * <p>
 * Currently this filesystem only prevents deletion of open files.
 */
public class WindowsFS extends HandleTrackingFS {
  final Map<Object,Integer> openFiles = new HashMap<>();
  // TODO: try to make this as realistic as possible... it depends e.g. how you
  // open files, if you map them, etc, if you can delete them (Uwe knows the rules)
  
  // TODO: add case-insensitivity
  
  /**
   * Create a new instance, wrapping the delegate filesystem to
   * act like Windows.
   * @param delegate delegate filesystem to wrap.
   */
  public WindowsFS(FileSystem delegate) {
    super("windows://", delegate);
  }
  
  /** 
   * Returns file "key" (e.g. inode) for the specified path 
   */
  private Object getKey(Path existing) throws IOException {
    BasicFileAttributeView view = Files.getFileAttributeView(existing, BasicFileAttributeView.class);
    BasicFileAttributes attributes = view.readAttributes();
    return attributes.fileKey();
  }

  @Override
  protected void onOpen(Path path, Object stream) throws IOException {
    synchronized (openFiles) {
      final Object key = getKey(path);
      // we have to read the key under the lock otherwise me might leak the openFile handle
      // if we concurrently delete or move this file.
      Integer v = openFiles.get(key);
      if (v != null) {
        v = Integer.valueOf(v.intValue()+1);
        openFiles.put(key, v);
      } else {
        openFiles.put(key, Integer.valueOf(1));
      }
    }
  }

  @Override
  protected void onClose(Path path, Object stream) throws IOException {
    Object key = getKey(path); // here we can read this outside of the lock
    synchronized (openFiles) {
      Integer v = openFiles.get(key);
      assert v != null;
      if (v != null) {
        if (v.intValue() == 1) {
          openFiles.remove(key);
        } else {
          v = Integer.valueOf(v.intValue()-1);
          openFiles.put(key, v);
        }
      }
    }
  }
  
  /** 
   * Checks that it's ok to delete {@code Path}. If the file
   * is still open, it throws IOException("access denied").
   */
  private void checkDeleteAccess(Path path) throws IOException {
    Object key = null;
    try {
      key = getKey(path);
    } catch (Throwable ignore) {
      // we don't care if the file doesn't exist
    } 

    if (key != null) {
      synchronized(openFiles) {
        if (openFiles.containsKey(key)) {
          throw new IOException("access denied: " + path);
        }
      }
    }
  }

  @Override
  public void delete(Path path) throws IOException {
    synchronized (openFiles) {
      checkDeleteAccess(path);
      super.delete(path);
    }
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    synchronized (openFiles) {
      checkDeleteAccess(source);
      super.move(source, target, options);
    }
  }

  @Override
  public boolean deleteIfExists(Path path) throws IOException {
    synchronized (openFiles) {
      checkDeleteAccess(path);
      return super.deleteIfExists(path);
    }
  }
}
