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
package org.apache.solr.hadoop;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SolrRecordWriter<K, V> extends RecordWriter<K, V> {
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public final static List<String> allowedConfigDirectories = new ArrayList<>(
      Arrays.asList(new String[] { "conf", "lib", "solr.xml" }));

  public final static Set<String> requiredConfigDirectories = new HashSet<>();
  
  static {
    requiredConfigDirectories.add("conf");
  }

  /**
   * Return the list of directories names that may be included in the
   * configuration data passed to the tasks.
   * 
   * @return an UnmodifiableList of directory names
   */
  public static List<String> getAllowedConfigDirectories() {
    return Collections.unmodifiableList(allowedConfigDirectories);
  }

  /**
   * check if the passed in directory is required to be present in the
   * configuration data set.
   * 
   * @param directory The directory to check
   * @return true if the directory is required.
   */
  public static boolean isRequiredConfigDirectory(final String directory) {
    return requiredConfigDirectories.contains(directory);
  }

  /** The path that the final index will be written to */

  /** The location in a local temporary directory that the index is built in. */

//  /**
//   * If true, create a zip file of the completed index in the final storage
//   * location A .zip will be appended to the final output name if it is not
//   * already present.
//   */
//  private boolean outputZipFile = false;

  private final HeartBeater heartBeater;
  private final BatchWriter batchWriter;
  private final List<SolrInputDocument> batch;
  private final int batchSize;
  private long numDocsWritten = 0;
  private long nextLogTime = System.nanoTime();

  private static HashMap<TaskID, Reducer<?,?,?,?>.Context> contextMap = new HashMap<>();
  
  public SolrRecordWriter(TaskAttemptContext context, Path outputShardDir, int batchSize) {
    this.batchSize = batchSize;
    this.batch = new ArrayList<>(batchSize);
    Configuration conf = context.getConfiguration();

    // setLogLevel("org.apache.solr.core", "WARN");
    // setLogLevel("org.apache.solr.update", "WARN");

    heartBeater = new HeartBeater(context);
    try {
      heartBeater.needHeartBeat();

      Path solrHomeDir = SolrRecordWriter.findSolrConfig(conf);
      FileSystem fs = outputShardDir.getFileSystem(conf);
      EmbeddedSolrServer solr = createEmbeddedSolrServer(solrHomeDir, fs, outputShardDir);
      batchWriter = new BatchWriter(solr, batchSize,
          context.getTaskAttemptID().getTaskID(),
          SolrOutputFormat.getSolrWriterThreadCount(conf),
          SolrOutputFormat.getSolrWriterQueueSize(conf));

    } catch (Exception e) {
      throw new IllegalStateException(String.format(Locale.ENGLISH,
          "Failed to initialize record writer for %s, %s", context.getJobName(), conf
              .get("mapred.task.id")), e);
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }

  public static EmbeddedSolrServer createEmbeddedSolrServer(Path solrHomeDir, FileSystem fs, Path outputShardDir)
      throws IOException {

    LOG.info("Creating embedded Solr server with solrHomeDir: " + solrHomeDir + ", fs: " + fs + ", outputShardDir: " + outputShardDir);

    Path solrDataDir = new Path(outputShardDir, "data");

    String dataDirStr = solrDataDir.toUri().toString();

    SolrResourceLoader loader = new SolrResourceLoader(Paths.get(solrHomeDir.toString()), null, null);

    LOG.info(String
        .format(Locale.ENGLISH, 
            "Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to solr.data.dir %s, with permdir %s",
            solrHomeDir, solrHomeDir.toUri(), loader.getInstancePath(),
            loader.getConfigDir(), dataDirStr, outputShardDir));

    // TODO: This is fragile and should be well documented
    System.setProperty("solr.directoryFactory", HdfsDirectoryFactory.class.getName()); 
    System.setProperty("solr.lock.type", DirectoryFactory.LOCK_TYPE_HDFS);
    System.setProperty("solr.hdfs.nrtcachingdirectory", "false");
    System.setProperty("solr.hdfs.blockcache.enabled", "false");
    System.setProperty("solr.autoCommit.maxTime", "600000");
    System.setProperty("solr.autoSoftCommit.maxTime", "-1");
    
    CoreContainer container = new CoreContainer(loader);
    container.load();

    SolrCore core = container.create("core1", ImmutableMap.of(CoreDescriptor.CORE_DATADIR, dataDirStr));
    
    if (!(core.getDirectoryFactory() instanceof HdfsDirectoryFactory)) {
      throw new UnsupportedOperationException(
          "Invalid configuration. Currently, the only DirectoryFactory supported is "
              + HdfsDirectoryFactory.class.getSimpleName());
    }

    EmbeddedSolrServer solr = new EmbeddedSolrServer(container, "core1");
    return solr;
  }

  public static void incrementCounter(TaskID taskId, String groupName, String counterName, long incr) {
    Reducer<?,?,?,?>.Context context = contextMap.get(taskId);
    if (context != null) {
      context.getCounter(groupName, counterName).increment(incr);
    }
  }

  public static void incrementCounter(TaskID taskId, Enum<?> counterName, long incr) {
    Reducer<?,?,?,?>.Context context = contextMap.get(taskId);
    if (context != null) {
      context.getCounter(counterName).increment(incr);
    }
  }

  public static void addReducerContext(Reducer<?,?,?,?>.Context context) {
    TaskID taskID = context.getTaskAttemptID().getTaskID();
    contextMap.put(taskID, context);
  }

  public static Path findSolrConfig(Configuration conf) throws IOException {
    // FIXME when mrunit supports the new cache apis
    //URI[] localArchives = context.getCacheArchives();
    Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
    for (Path unpackedDir : localArchives) {
      if (unpackedDir.getName().equals(SolrOutputFormat.getZipName(conf))) {
        LOG.info("Using this unpacked directory as solr home: {}", unpackedDir);
        return unpackedDir;
      }
    }
    throw new IOException(String.format(Locale.ENGLISH,
        "No local cache archives, where is %s:%s", SolrOutputFormat
            .getSetupOk(), SolrOutputFormat.getZipName(conf)));
  }

  /**
   * Write a record. This method accumulates records in to a batch, and when
   * {@link #batchSize} items are present flushes it to the indexer. The writes
   * can take a substantial amount of time, depending on {@link #batchSize}. If
   * there is heavy disk contention the writes may take more than the 600 second
   * default timeout.
   */
  @Override
  public void write(K key, V value) throws IOException {
    heartBeater.needHeartBeat();
    try {
      try {
        SolrInputDocumentWritable sidw = (SolrInputDocumentWritable) value;
        batch.add(sidw.getSolrInputDocument());
        if (batch.size() >= batchSize) {
          batchWriter.queueBatch(batch);
          numDocsWritten += batch.size();
          if (System.nanoTime() >= nextLogTime) {
            LOG.info("docsWritten: {}", numDocsWritten);
            nextLogTime += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
          }
          batch.clear();
        }
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
    } finally {
      heartBeater.cancelHeartBeat();
    }

  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    if (context != null) {
      heartBeater.setProgress(context);
    }
    try {
      heartBeater.needHeartBeat();
      if (batch.size() > 0) {
        batchWriter.queueBatch(batch);
        numDocsWritten += batch.size();
        batch.clear();
      }
      LOG.info("docsWritten: {}", numDocsWritten);
      batchWriter.close(context);
//      if (outputZipFile) {
//        context.setStatus("Writing Zip");
//        packZipFile(); // Written to the perm location
//      } else {
//        context.setStatus("Copying Index");
//        fs.completeLocalOutput(perm, temp); // copy to dfs
//      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    } finally {
      heartBeater.cancelHeartBeat();
      heartBeater.close();
//      File tempFile = new File(temp.toString());
//      if (tempFile.exists()) {
//        FileUtils.forceDelete(new File(temp.toString()));
//      }
    }

    context.setStatus("Done");
  }

//  private void packZipFile() throws IOException {
//    FSDataOutputStream out = null;
//    ZipOutputStream zos = null;
//    int zipCount = 0;
//    LOG.info("Packing zip file for " + perm);
//    try {
//      out = fs.create(perm, false);
//      zos = new ZipOutputStream(out);
//
//      String name = perm.getName().replaceAll(".zip$", "");
//      LOG.info("adding index directory" + temp);
//      zipCount = zipDirectory(conf, zos, name, temp.toString(), temp);
//      /**
//      for (String configDir : allowedConfigDirectories) {
//        if (!isRequiredConfigDirectory(configDir)) {
//          continue;
//        }
//        final Path confPath = new Path(solrHome, configDir);
//        LOG.info("adding configdirectory" + confPath);
//
//        zipCount += zipDirectory(conf, zos, name, solrHome.toString(), confPath);
//      }
//      **/
//    } catch (Throwable ohFoo) {
//      LOG.error("packZipFile exception", ohFoo);
//      if (ohFoo instanceof RuntimeException) {
//        throw (RuntimeException) ohFoo;
//      }
//      if (ohFoo instanceof IOException) {
//        throw (IOException) ohFoo;
//      }
//      throw new IOException(ohFoo);
//
//    } finally {
//      if (zos != null) {
//        if (zipCount == 0) { // If no entries were written, only close out, as
//                             // the zip will throw an error
//          LOG.error("No entries written to zip file " + perm);
//          fs.delete(perm, false);
//          // out.close();
//        } else {
//          LOG.info(String.format("Wrote %d items to %s for %s", zipCount, perm,
//              temp));
//          zos.close();
//        }
//      }
//    }
//  }
//
//  /**
//   * Write a file to a zip output stream, removing leading path name components
//   * from the actual file name when creating the zip file entry.
//   * 
//   * The entry placed in the zip file is <code>baseName</code>/
//   * <code>relativePath</code>, where <code>relativePath</code> is constructed
//   * by removing a leading <code>root</code> from the path for
//   * <code>itemToZip</code>.
//   * 
//   * If <code>itemToZip</code> is an empty directory, it is ignored. If
//   * <code>itemToZip</code> is a directory, the contents of the directory are
//   * added recursively.
//   * 
//   * @param zos The zip output stream
//   * @param baseName The base name to use for the file name entry in the zip
//   *        file
//   * @param root The path to remove from <code>itemToZip</code> to make a
//   *        relative path name
//   * @param itemToZip The path to the file to be added to the zip file
//   * @return the number of entries added
//   * @throws IOException
//   */
//  static public int zipDirectory(final Configuration conf,
//      final ZipOutputStream zos, final String baseName, final String root,
//      final Path itemToZip) throws IOException {
//    LOG
//        .info(String
//            .format("zipDirectory: %s %s %s", baseName, root, itemToZip));
//    LocalFileSystem localFs = FileSystem.getLocal(conf);
//    int count = 0;
//
//    final FileStatus itemStatus = localFs.getFileStatus(itemToZip);
//    if (itemStatus.isDirectory()) {
//      final FileStatus[] statai = localFs.listStatus(itemToZip);
//
//      // Add a directory entry to the zip file
//      final String zipDirName = relativePathForZipEntry(itemToZip.toUri()
//          .getPath(), baseName, root);
//      final ZipEntry dirZipEntry = new ZipEntry(zipDirName
//          + Path.SEPARATOR_CHAR);
//      LOG.info(String.format("Adding directory %s to zip", zipDirName));
//      zos.putNextEntry(dirZipEntry);
//      zos.closeEntry();
//      count++;
//
//      if (statai == null || statai.length == 0) {
//        LOG.info(String.format("Skipping empty directory %s", itemToZip));
//        return count;
//      }
//      for (FileStatus status : statai) {
//        count += zipDirectory(conf, zos, baseName, root, status.getPath());
//      }
//      LOG.info(String.format("Wrote %d entries for directory %s", count,
//          itemToZip));
//      return count;
//    }
//
//    final String inZipPath = relativePathForZipEntry(itemToZip.toUri()
//        .getPath(), baseName, root);
//
//    if (inZipPath.length() == 0) {
//      LOG.warn(String.format("Skipping empty zip file path for %s (%s %s)",
//          itemToZip, root, baseName));
//      return 0;
//    }
//
//    // Take empty files in case the place holder is needed
//    FSDataInputStream in = null;
//    try {
//      in = localFs.open(itemToZip);
//      final ZipEntry ze = new ZipEntry(inZipPath);
//      ze.setTime(itemStatus.getModificationTime());
//      // Comments confuse looking at the zip file
//      // ze.setComment(itemToZip.toString());
//      zos.putNextEntry(ze);
//
//      IOUtils.copyBytes(in, zos, conf, false);
//      zos.closeEntry();
//      LOG.info(String.format("Wrote %d entries for file %s", count, itemToZip));
//      return 1;
//    } finally {
//      in.close();
//    }
//
//  }
//
//  static String relativePathForZipEntry(final String rawPath,
//      final String baseName, final String root) {
//    String relativePath = rawPath.replaceFirst(Pattern.quote(root.toString()),
//        "");
//    LOG.info(String.format("RawPath %s, baseName %s, root %s, first %s",
//        rawPath, baseName, root, relativePath));
//
//    if (relativePath.startsWith(Path.SEPARATOR)) {
//      relativePath = relativePath.substring(1);
//    }
//    LOG.info(String.format(
//        "RawPath %s, baseName %s, root %s, post leading slash %s", rawPath,
//        baseName, root, relativePath));
//    if (relativePath.isEmpty()) {
//      LOG.warn(String.format(
//          "No data after root (%s) removal from raw path %s", root, rawPath));
//      return baseName;
//    }
//    // Construct the path that will be written to the zip file, including
//    // removing any leading '/' characters
//    String inZipPath = baseName + Path.SEPARATOR_CHAR + relativePath;
//
//    LOG.info(String.format("RawPath %s, baseName %s, root %s, inZip 1 %s",
//        rawPath, baseName, root, inZipPath));
//    if (inZipPath.startsWith(Path.SEPARATOR)) {
//      inZipPath = inZipPath.substring(1);
//    }
//    LOG.info(String.format("RawPath %s, baseName %s, root %s, inZip 2 %s",
//        rawPath, baseName, root, inZipPath));
//
//    return inZipPath;
//
//  }
//  
  /*
  static boolean setLogLevel(String packageName, String level) {
    Log logger = LogFactory.getLog(packageName);
    if (logger == null) {
      return false;
    }
    // look for: org.apache.commons.logging.impl.SLF4JLocationAwareLog
    LOG.warn("logger class:"+logger.getClass().getName());
    if (logger instanceof Log4JLogger) {
      process(((Log4JLogger) logger).getLogger(), level);
      return true;
    }
    if (logger instanceof Jdk14Logger) {
      process(((Jdk14Logger) logger).getLogger(), level);
      return true;
    }
    return false;
  }

  public static void process(org.apache.log4j.Logger log, String level) {
    if (level != null) {
      log.setLevel(org.apache.log4j.Level.toLevel(level));
    }
  }

  public static void process(java.util.logging.Logger log, String level) {
    if (level != null) {
      log.setLevel(java.util.logging.Level.parse(level));
    }
  }
  */
}
