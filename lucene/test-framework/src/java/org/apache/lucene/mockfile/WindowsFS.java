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
  // This map also supports fileKey -> Path -> counts
  // which is important to effectively support renames etc.
  // in the rename case we have to transfer ownership but need to make sure we only transfer ownership for
  // the path we rename ie. hardlinks will still resolve to the same key
  final Map<Object,Map<Path, Integer>> openFiles = new HashMap<>();
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
      Map<Path, Integer> pathMap = openFiles.computeIfAbsent(key, k -> new HashMap<>());
      pathMap.put(path, pathMap.computeIfAbsent(path, p -> 0).intValue() +1);
    }
  }

  @Override
  protected void onClose(Path path, Object stream) throws IOException {
    Object key = getKey(path); // here we can read this outside of the lock
    synchronized (openFiles) {
      Map<Path, Integer> pathMap = openFiles.get(key);
      assert pathMap != null;
      assert pathMap.containsKey(path);
      Integer v = pathMap.get(path);
      if (v != null) {
        if (v.intValue() == 1) {
          pathMap.remove(path);
        } else {
          v = Integer.valueOf(v.intValue()-1);
          pathMap.put(path, v);
        }
      }
      if (pathMap.isEmpty()) {
        openFiles.remove(key);
      }
    }
  }

  private Object getKeyOrNull(Path path) {
    try {
      return getKey(path);
    } catch (Exception ignore) {
      // we don't care if the file doesn't exist
    }
    return null;
  }
  
  /** 
   * Checks that it's ok to delete {@code Path}. If the file
   * is still open, it throws IOException("access denied").
   */
  private void checkDeleteAccess(Path path) throws IOException {
    Object key = getKeyOrNull(path);
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
      Object key = getKeyOrNull(target);
      super.move(source, target, options);
      if (key != null) {
        Object newKey = getKey(target);
        if (newKey.equals(key) == false) {
          // we need to transfer ownership here if we have open files on this file since the getKey() method will
          // return a different i-node next time we call it with the target path and our onClose method will
          // trip an assert
          Map<Path, Integer> map = openFiles.get(key);
          if (map != null) {
            Integer v = map.remove(target);
            if (v != null) {
              Map<Path, Integer> pathIntegerMap = openFiles.computeIfAbsent(newKey, k -> new HashMap<>());
              Integer existingValue = pathIntegerMap.getOrDefault(target, 0);
              pathIntegerMap.put(target, existingValue + v);
            }
            if (map.isEmpty()) {
              openFiles.remove(key);
            }
          }
        }
      }
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
