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

package org.apache.solr.core.backup.repository;

import java.io.IOException;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HdfsBackupRepositoryTest {

  @Test(expected = NullPointerException.class)
  public void testHdfsHomePropertyMissing() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository())  {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      hdfsBackupRepository.init(namedList);
    }
  }

  @Test
  public void testHdfsHomePropertySet() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add(HdfsDirectoryFactory.HDFS_HOME, "hdfs://localhost");
      hdfsBackupRepository.init(namedList);
    }
  }

  @Test(expected = ClassCastException.class)
  public void testCopyBufferSizeNonNumeric() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add("solr.hdfs.buffer.size", "xyz");
      hdfsBackupRepository.init(namedList);
    }
  }

  @Test(expected = ClassCastException.class)
  public void testCopyBufferSizeWrongType() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add("solr.hdfs.buffer.size", "8192");
      hdfsBackupRepository.init(namedList);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCopyBufferSizeNegative() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add("solr.hdfs.buffer.size", -1);
      hdfsBackupRepository.init(namedList);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCopyBufferSizeZero() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add("solr.hdfs.buffer.size", 0);
      hdfsBackupRepository.init(namedList);
    }
  }

  @Test
  public void testCopyBufferSet() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add(HdfsDirectoryFactory.HDFS_HOME, "hdfs://localhost");
      namedList.add("solr.hdfs.buffer.size", 32768);
      hdfsBackupRepository.init(namedList);
      assertEquals(hdfsBackupRepository.copyBufferSize, 32768);
    }
  }

  @Test
  public void testCopyBufferDefaultSize() throws IOException {
    try (HdfsBackupRepository hdfsBackupRepository = new HdfsBackupRepository()) {
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      namedList.add(HdfsDirectoryFactory.HDFS_HOME, "hdfs://localhost");
      hdfsBackupRepository.init(namedList);
      assertEquals(hdfsBackupRepository.copyBufferSize, HdfsDirectory.DEFAULT_BUFFER_SIZE);
    }
  }
}
