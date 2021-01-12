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
package org.apache.lucene.replicator;

/**
 * Describes a file in a {@link Revision}. A file has a source, which allows a single revision to
 * contain files from multiple sources (e.g. multiple indexes).
 *
 * @lucene.experimental
 */
public class RevisionFile {

  /** The name of the file. */
  public final String fileName;

  /** The size of the file denoted by {@link #fileName}. */
  public long size = -1;

  /** Constructor with the given file name. */
  public RevisionFile(String fileName) {
    if (fileName == null || fileName.isEmpty()) {
      throw new IllegalArgumentException("fileName must not be null or empty");
    }
    this.fileName = fileName;
  }

  @Override
  public boolean equals(Object obj) {
    RevisionFile other = (RevisionFile) obj;
    return fileName.equals(other.fileName) && size == other.size;
  }

  @Override
  public int hashCode() {
    return fileName.hashCode() ^ (int) (size ^ (size >>> 32));
  }

  @Override
  public String toString() {
    return "fileName=" + fileName + " size=" + size;
  }
}
