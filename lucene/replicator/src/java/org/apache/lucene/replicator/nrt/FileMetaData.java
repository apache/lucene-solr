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

/**
 * Holds metadata details about a single file that we use to confirm two files (one remote, one
 * local) are in fact "identical".
 *
 * @lucene.experimental
 */
public class FileMetaData {

  // Header and footer of the file must be identical between primary and replica to consider the
  // files equal:
  public final byte[] header;
  public final byte[] footer;

  public final long length;

  // Used to ensure no bit flips when copying the file:
  public final long checksum;

  public FileMetaData(byte[] header, byte[] footer, long length, long checksum) {
    this.header = header;
    this.footer = footer;
    this.length = length;
    this.checksum = checksum;
  }

  @Override
  public String toString() {
    return "FileMetaData(length=" + length + ")";
  }
}
