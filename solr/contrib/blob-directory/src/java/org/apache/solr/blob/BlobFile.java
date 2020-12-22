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

package org.apache.solr.blob;

import java.util.Objects;

/** A file in Blob, consisting of a name, size, and checksum. */
public class BlobFile {
  protected final String fileName;
  protected final long size;
  protected final long checksum;

  public BlobFile(String fileName, long size, long checksum) {
    this.fileName = fileName;
    this.size = size;
    this.checksum = checksum;
  }

  public String fileName() {
    return fileName;
  }

  public long size() {
    return size;
  }

  public long checksum() {
    return checksum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BlobFile)) return false;
    BlobFile impl = (BlobFile) o;
    return size == impl.size && checksum == impl.checksum && fileName.equals(impl.fileName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, size, checksum);
  }

  @Override
  public String toString() {
    return fileName + " size=" + size + " chk=" + checksum;
  }
}
