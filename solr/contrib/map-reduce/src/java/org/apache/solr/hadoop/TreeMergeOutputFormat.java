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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.misc.IndexMergeTool;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * See {@link IndexMergeTool}.
 */
public class TreeMergeOutputFormat extends FileOutputFormat<Text, NullWritable> {
  
  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException {
    Utils.getLogConfigFile(context.getConfiguration());
    Path workDir = getDefaultWorkFile(context, "");
    return new TreeMergeRecordWriter(context, workDir);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TreeMergeRecordWriter extends RecordWriter<Text,NullWritable> {
    
    private final Path workDir;
    private final List<Path> shards = new ArrayList();
    private final HeartBeater heartBeater;
    private final TaskAttemptContext context;
    
    private static final Logger LOG = LoggerFactory.getLogger(TreeMergeRecordWriter.class);

    public TreeMergeRecordWriter(TaskAttemptContext context, Path workDir) {
      this.workDir = new Path(workDir, "data/index");
      this.heartBeater = new HeartBeater(context);
      this.context = context;
    }
    
    @Override
    public void write(Text key, NullWritable value) {
      LOG.info("map key: {}", key);
      heartBeater.needHeartBeat();
      try {
        Path path = new Path(key.toString());
        shards.add(path);
      } finally {
        heartBeater.cancelHeartBeat();
      }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException {
      LOG.debug("Task " + context.getTaskAttemptID() + " merging into dstDir: " + workDir + ", srcDirs: " + shards);
      writeShardNumberFile(context);      
      heartBeater.needHeartBeat();
      try {
        Directory mergedIndex = new HdfsDirectory(workDir, NoLockFactory.getNoLockFactory(), context.getConfiguration());
        
        // TODO: shouldn't we pull the Version from the solrconfig.xml?
        IndexWriterConfig writerConfig = new IndexWriterConfig(null)
            .setOpenMode(OpenMode.CREATE).setUseCompoundFile(false)
            //.setMergePolicy(mergePolicy) // TODO: grab tuned MergePolicy from solrconfig.xml?
            //.setMergeScheduler(...) // TODO: grab tuned MergeScheduler from solrconfig.xml?
            ;
          
        if (LOG.isDebugEnabled()) {
          writerConfig.setInfoStream(System.out);
        }
//        writerConfig.setRAMBufferSizeMB(100); // improve performance
//        writerConfig.setMaxThreadStates(1);
        
        // disable compound file to improve performance
        // also see http://lucene.472066.n3.nabble.com/Questions-on-compound-file-format-td489105.html
        // also see defaults in SolrIndexConfig
        MergePolicy mergePolicy = writerConfig.getMergePolicy();
        LOG.debug("mergePolicy was: {}", mergePolicy);
        if (mergePolicy instanceof TieredMergePolicy) {
          ((TieredMergePolicy) mergePolicy).setNoCFSRatio(0.0);
//          ((TieredMergePolicy) mergePolicy).setMaxMergeAtOnceExplicit(10000);          
//          ((TieredMergePolicy) mergePolicy).setMaxMergeAtOnce(10000);       
//          ((TieredMergePolicy) mergePolicy).setSegmentsPerTier(10000);
        } else if (mergePolicy instanceof LogMergePolicy) {
          ((LogMergePolicy) mergePolicy).setNoCFSRatio(0.0);
        }
        LOG.info("Using mergePolicy: {}", mergePolicy);
        
        IndexWriter writer = new IndexWriter(mergedIndex, writerConfig);
        
        Directory[] indexes = new Directory[shards.size()];
        for (int i = 0; i < shards.size(); i++) {
          indexes[i] = new HdfsDirectory(shards.get(i), NoLockFactory.getNoLockFactory(), context.getConfiguration());
        }

        context.setStatus("Logically merging " + shards.size() + " shards into one shard");
        LOG.info("Logically merging " + shards.size() + " shards into one shard: " + workDir);
        long start = System.nanoTime();
        
        writer.addIndexes(indexes); 
        // TODO: avoid intermediate copying of files into dst directory; rename the files into the dir instead (cp -> rename) 
        // This can improve performance and turns this phase into a true "logical" merge, completing in constant time.
        // See https://issues.apache.org/jira/browse/LUCENE-4746
        
        if (LOG.isDebugEnabled()) {
          context.getCounter(SolrCounters.class.getName(), SolrCounters.LOGICAL_TREE_MERGE_TIME.toString()).increment(System.currentTimeMillis() - start);
        }
        float secs = (System.nanoTime() - start) / (float)(10^9);
        LOG.info("Logical merge took {} secs", secs);        
        int maxSegments = context.getConfiguration().getInt(TreeMergeMapper.MAX_SEGMENTS_ON_TREE_MERGE, Integer.MAX_VALUE);
        context.setStatus("Optimizing Solr: forcing mtree merge down to " + maxSegments + " segments");
        LOG.info("Optimizing Solr: forcing tree merge down to {} segments", maxSegments);
        start = System.nanoTime();
        if (maxSegments < Integer.MAX_VALUE) {
          writer.forceMerge(maxSegments); 
          // TODO: consider perf enhancement for no-deletes merges: bulk-copy the postings data 
          // see http://lucene.472066.n3.nabble.com/Experience-with-large-merge-factors-tp1637832p1647046.html
        }
        if (LOG.isDebugEnabled()) {
          context.getCounter(SolrCounters.class.getName(), SolrCounters.PHYSICAL_TREE_MERGE_TIME.toString()).increment(System.currentTimeMillis() - start);
        }
        secs = (System.nanoTime() - start) / (float)(10^9);
        LOG.info("Optimizing Solr: done forcing tree merge down to {} segments in {} secs", maxSegments, secs);
        
        start = System.nanoTime();
        LOG.info("Optimizing Solr: Closing index writer");
        writer.close();
        secs = (System.nanoTime() - start) / (float)(10^9);
        LOG.info("Optimizing Solr: Done closing index writer in {} secs", secs);
        context.setStatus("Done");
      } finally {
        heartBeater.cancelHeartBeat();
        heartBeater.close();
      }
    }

    /*
     * For background see MapReduceIndexerTool.renameTreeMergeShardDirs()
     * 
     * Also see MapReduceIndexerTool.run() method where it uses
     * NLineInputFormat.setNumLinesPerSplit(job, options.fanout)
     */
    private void writeShardNumberFile(TaskAttemptContext context) throws IOException {
      Preconditions.checkArgument(shards.size() > 0);
      String shard = shards.get(0).getParent().getParent().getName(); // move up from "data/index"
      String taskId = shard.substring("part-m-".length(), shard.length()); // e.g. part-m-00001
      int taskNum = Integer.parseInt(taskId);
      int outputShardNum = taskNum / shards.size();
      LOG.debug("Merging into outputShardNum: " + outputShardNum + " from taskId: " + taskId);
      Path shardNumberFile = new Path(workDir.getParent().getParent(), TreeMergeMapper.SOLR_SHARD_NUMBER);
      OutputStream out = shardNumberFile.getFileSystem(context.getConfiguration()).create(shardNumberFile);
      Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      writer.write(String.valueOf(outputShardNum));
      writer.flush();
      writer.close();
    }    
  }
}
