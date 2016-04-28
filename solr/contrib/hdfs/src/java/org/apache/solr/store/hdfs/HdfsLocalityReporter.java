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
package org.apache.solr.store.hdfs;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsLocalityReporter implements SolrInfoMBean {
  public static final String LOCALITY_BYTES_TOTAL = "locality.bytes.total";
  public static final String LOCALITY_BYTES_LOCAL = "locality.bytes.local";
  public static final String LOCALITY_BYTES_RATIO = "locality.bytes.ratio";
  public static final String LOCALITY_BLOCKS_TOTAL = "locality.blocks.total";
  public static final String LOCALITY_BLOCKS_LOCAL = "locality.blocks.local";
  public static final String LOCALITY_BLOCKS_RATIO = "locality.blocks.ratio";

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String hostname;
  private final ConcurrentMap<HdfsDirectory,ConcurrentMap<FileStatus,BlockLocation[]>> cache;

  public HdfsLocalityReporter() {
    cache = new ConcurrentHashMap<>();
  }

  /**
   * Set the host name to use when determining locality
   * @param hostname The name of this host; should correspond to what HDFS Data Nodes think this is.
   */
  public void setHost(String hostname) {
    this.hostname = hostname;
  }
  
  @Override
  public String getName() {
    return "hdfs-locality";
  }

  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }

  @Override
  public String getDescription() {
    return "Provides metrics for HDFS data locality.";
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  /**
   * Provide statistics on HDFS block locality, both in terms of bytes and block counts.
   */
  @Override
  public NamedList getStatistics() {
    long totalBytes = 0;
    long localBytes = 0;
    int totalCount = 0;
    int localCount = 0;

    for (Iterator<HdfsDirectory> iterator = cache.keySet().iterator(); iterator.hasNext();) {
      HdfsDirectory hdfsDirectory = iterator.next();

      if (hdfsDirectory.isClosed()) {
        iterator.remove();
      } else {
        try {
          refreshDirectory(hdfsDirectory);
          Map<FileStatus,BlockLocation[]> blockMap = cache.get(hdfsDirectory);

          // For every block in every file in this directory, count it
          for (BlockLocation[] locations : blockMap.values()) {
            for (BlockLocation bl : locations) {
              totalBytes += bl.getLength();
              totalCount++;

              if (Arrays.asList(bl.getHosts()).contains(hostname)) {
                localBytes += bl.getLength();
                localCount++;
              }
            }
          }
        } catch (IOException e) {
          logger.warn("Could not retrieve locality information for {} due to exception: {}",
              hdfsDirectory.getHdfsDirPath(), e);
        }
      }
    }

    return createStatistics(totalBytes, localBytes, totalCount, localCount);
  }

  /**
   * Generate a statistics object based on the given measurements for all files monitored by this reporter.
   * 
   * @param totalBytes
   *          The total bytes used
   * @param localBytes
   *          The amount of bytes found on local nodes
   * @param totalCount
   *          The total block count
   * @param localCount
   *          The amount of blocks found on local nodes
   * @return HDFS block locality statistics
   */
  private NamedList<Number> createStatistics(long totalBytes, long localBytes, int totalCount, int localCount) {
    NamedList<Number> statistics = new SimpleOrderedMap<Number>();

    statistics.add(LOCALITY_BYTES_TOTAL, totalBytes);
    statistics.add(LOCALITY_BYTES_LOCAL, localBytes);
    if (localBytes == 0) {
      statistics.add(LOCALITY_BYTES_RATIO, 0);
    } else {
      statistics.add(LOCALITY_BYTES_RATIO, localBytes / (double) totalBytes);
    }
    statistics.add(LOCALITY_BLOCKS_TOTAL, totalCount);
    statistics.add(LOCALITY_BLOCKS_LOCAL, localCount);
    if (localCount == 0) {
      statistics.add(LOCALITY_BLOCKS_RATIO, 0);
    } else {
      statistics.add(LOCALITY_BLOCKS_RATIO, localCount / (double) totalCount);
    }

    return statistics;
  }

  /**
   * Add a directory for block locality reporting. This directory will continue to be checked until its close method has
   * been called.
   * 
   * @param dir
   *          The directory to keep metrics on.
   */
  public void registerDirectory(HdfsDirectory dir) {
    logger.info("Registering direcotry {} for locality metrics.", dir.getHdfsDirPath().toString());
    cache.put(dir, new ConcurrentHashMap<FileStatus, BlockLocation[]>());
  }

  /**
   * Update the cached block locations for the given directory. This includes deleting any files that no longer exist in
   * the file system and adding any new files that have shown up.
   * 
   * @param dir
   *          The directory to refresh
   * @throws IOException
   *           If there is a problem getting info from HDFS
   */
  private void refreshDirectory(HdfsDirectory dir) throws IOException {
    Map<FileStatus,BlockLocation[]> directoryCache = cache.get(dir);
    Set<FileStatus> cachedStatuses = directoryCache.keySet();

    FileSystem fs = dir.getFileSystem();
    FileStatus[] statuses = fs.listStatus(dir.getHdfsDirPath());
    List<FileStatus> statusList = Arrays.asList(statuses);

    logger.debug("Updating locality information for: {}", statusList);

    // Keep only the files that still exist
    cachedStatuses.retainAll(statusList);

    // Fill in missing entries in the cache
    for (FileStatus status : statusList) {
      if (!status.isDirectory() && !directoryCache.containsKey(status)) {
        BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());
        directoryCache.put(status, locations);
      }
    }
  }
}
