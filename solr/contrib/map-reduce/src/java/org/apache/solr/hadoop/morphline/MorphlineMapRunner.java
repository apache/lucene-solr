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
package org.apache.solr.hadoop.morphline;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.hadoop.HdfsFileFieldNames;
import org.apache.solr.hadoop.PathParts;
import org.apache.solr.hadoop.Utils;
import org.apache.solr.morphlines.solr.DocumentLoader;
import org.apache.solr.morphlines.solr.SolrLocator;
import org.apache.solr.morphlines.solr.SolrMorphlineContext;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.Beta;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Internal helper for {@link MorphlineMapper} and dryRun mode; This API is for *INTERNAL* use only
 * and should not be considered public.
 */
@Beta
public final class MorphlineMapRunner {

  private MorphlineContext morphlineContext;
  private Command morphline;
  private IndexSchema schema;
  private Map<String, String> commandLineMorphlineHeaders;
  private boolean disableFileOpen;
  private String morphlineFileAndId;
  private final Timer elapsedTime;   
  
  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  
  /**
   * Morphline variables can be passed from the CLI to the Morphline, e.g.:
   * hadoop ... -D morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";
  
  /**
   * Headers, including MIME types, can also explicitly be passed by force from the CLI to Morphline, e.g:
   * hadoop ... -D morphlineField._attachment_mimetype=text/csv
   */
  public static final String MORPHLINE_FIELD_PREFIX = "morphlineField.";
  
  /**
   * Flag to disable reading of file contents if indexing just file metadata is sufficient. 
   * This improves performance and confidentiality.
   */
  public static final String DISABLE_FILE_OPEN = "morphlineDisableFileOpen";
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  MorphlineContext getMorphlineContext() {
    return morphlineContext;
  }
  
  IndexSchema getSchema() {
    return schema;
  }

  public MorphlineMapRunner(Configuration configuration, DocumentLoader loader, String solrHomeDir) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("CWD is {}", new File(".").getCanonicalPath());
      TreeMap map = new TreeMap();
      for (Map.Entry<String,String> entry : configuration) {
        map.put(entry.getKey(), entry.getValue());
      }
      LOG.trace("Configuration:\n" +
          map.entrySet().stream().map(Object::toString).collect(Collectors.joining("\n")));
    }
    
    FaultTolerance faultTolerance = new FaultTolerance(
        configuration.getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false), 
        configuration.getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
        configuration.get(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES, SolrServerException.class.getName())        
        );
    
    morphlineContext = new SolrMorphlineContext.Builder()
      .setDocumentLoader(loader)
      .setExceptionHandler(faultTolerance)
      .setMetricRegistry(new MetricRegistry())
      .build();

    class MySolrLocator extends SolrLocator { // trick to access protected ctor
      public MySolrLocator(MorphlineContext ctx) {
        super(ctx);
      }
    }

    SolrLocator locator = new MySolrLocator(morphlineContext);
    locator.setSolrHomeDir(solrHomeDir);
    schema = locator.getIndexSchema();

    // rebuild context, now with schema
    morphlineContext = new SolrMorphlineContext.Builder()
      .setIndexSchema(schema)
      .setDocumentLoader(loader)
      .setExceptionHandler(faultTolerance)
      .setMetricRegistry(morphlineContext.getMetricRegistry())
      .build();

    String morphlineFile = configuration.get(MORPHLINE_FILE_PARAM);
    String morphlineId = configuration.get(MORPHLINE_ID_PARAM);
    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
    }
    Map morphlineVariables = new HashMap();
    for (Map.Entry<String, String> entry : configuration) {
      String variablePrefix = MORPHLINE_VARIABLE_PARAM + ".";
      if (entry.getKey().startsWith(variablePrefix)) {
        morphlineVariables.put(entry.getKey().substring(variablePrefix.length()), entry.getValue());
      }
    }
    Config override = ConfigFactory.parseMap(morphlineVariables);
    morphline = new Compiler().compile(new File(morphlineFile), morphlineId, morphlineContext, null, override);
    morphlineFileAndId = morphlineFile + "@" + morphlineId;
    
    disableFileOpen = configuration.getBoolean(DISABLE_FILE_OPEN, false);
    LOG.debug("disableFileOpen: {}", disableFileOpen);
        
    commandLineMorphlineHeaders = new HashMap();
    for (Map.Entry<String,String> entry : configuration) {     
      if (entry.getKey().startsWith(MORPHLINE_FIELD_PREFIX)) {
        commandLineMorphlineHeaders.put(entry.getKey().substring(MORPHLINE_FIELD_PREFIX.length()), entry.getValue());
      }
    }
    LOG.debug("Headers, including MIME types, passed by force from the CLI to morphline: {}", commandLineMorphlineHeaders);

    String metricName = MetricRegistry.name(Utils.getShortClassName(getClass()), Metrics.ELAPSED_TIME);
    this.elapsedTime = morphlineContext.getMetricRegistry().timer(metricName);
    Notifications.notifyBeginTransaction(morphline);
  }

  /**
   * Extract content from the path specified in the value. Key is useless.
   */
  public void map(String value, Configuration configuration, Context context) throws IOException {
    LOG.info("Processing file {}", value);
    InputStream in = null;
    Record record = null;
    Timer.Context timerContext = elapsedTime.time();
    try {
      PathParts parts = new PathParts(value.toString(), configuration);
      record = getRecord(parts);
      if (record == null) {
        return; // ignore
      }
      for (Map.Entry<String, String> entry : commandLineMorphlineHeaders.entrySet()) {
        record.replaceValues(entry.getKey(), entry.getValue());
      }
      long fileLength = parts.getFileStatus().getLen();
      if (disableFileOpen) {
        in = new ByteArrayInputStream(new byte[0]);
      } else {
        in = new BufferedInputStream(parts.getFileSystem().open(parts.getUploadPath()));
      }
      record.put(Fields.ATTACHMENT_BODY, in);
      Notifications.notifyStartSession(morphline);
      if (!morphline.process(record)) {
        LOG.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
      }
      if (context != null) {
        context.getCounter(MorphlineCounters.class.getName(), MorphlineCounters.FILES_READ.toString()).increment(1);
        context.getCounter(MorphlineCounters.class.getName(), MorphlineCounters.FILE_BYTES_READ.toString()).increment(fileLength);
      }
    } catch (Exception e) {
      LOG.error("Unable to process file " + value, e);
      if (context != null) {
        context.getCounter(getClass().getName() + ".errors", e.getClass().getName()).increment(1);
      }
      morphlineContext.getExceptionHandler().handleException(e, record);
    } finally {
      timerContext.stop();
      if (in != null) {
        in.close();
      }
    }
  }
  
  protected Record getRecord(PathParts parts) {
    FileStatus stats;
    try {
      stats = parts.getFileStatus();
    } catch (IOException e) {
      stats = null;
    }
    if (stats == null) {
      LOG.warn("Ignoring file that somehow has become unavailable since the job was submitted: {}",
          parts.getUploadURL());
      return null;
    }
    
    Record headers = new Record();
    //headers.put(getSchema().getUniqueKeyField().getName(), parts.getId()); // use HDFS file path as docId if no docId is specified
    headers.put(Fields.BASE_ID, parts.getId()); // with sanitizeUniqueKey command, use HDFS file path as docId if no docId is specified
    headers.put(Fields.ATTACHMENT_NAME, parts.getName()); // Tika can use the file name in guessing the right MIME type
    
    // enable indexing and storing of file meta data in Solr
    headers.put(HdfsFileFieldNames.FILE_UPLOAD_URL, parts.getUploadURL());
    headers.put(HdfsFileFieldNames.FILE_DOWNLOAD_URL, parts.getDownloadURL());
    headers.put(HdfsFileFieldNames.FILE_SCHEME, parts.getScheme()); 
    headers.put(HdfsFileFieldNames.FILE_HOST, parts.getHost()); 
    headers.put(HdfsFileFieldNames.FILE_PORT, String.valueOf(parts.getPort())); 
    headers.put(HdfsFileFieldNames.FILE_PATH, parts.getURIPath()); 
    headers.put(HdfsFileFieldNames.FILE_NAME, parts.getName());     
    headers.put(HdfsFileFieldNames.FILE_LAST_MODIFIED, String.valueOf(stats.getModificationTime())); // FIXME also add in SpoolDirectorySource
    headers.put(HdfsFileFieldNames.FILE_LENGTH, String.valueOf(stats.getLen())); // FIXME also add in SpoolDirectorySource
    headers.put(HdfsFileFieldNames.FILE_OWNER, stats.getOwner());
    headers.put(HdfsFileFieldNames.FILE_GROUP, stats.getGroup());
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_USER, stats.getPermission().getUserAction().SYMBOL);
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_GROUP, stats.getPermission().getGroupAction().SYMBOL);
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_OTHER, stats.getPermission().getOtherAction().SYMBOL);
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_STICKYBIT, String.valueOf(stats.getPermission().getStickyBit()));
    // TODO: consider to add stats.getAccessTime(), stats.getReplication(), stats.isSymlink(), stats.getBlockSize()
    
    return headers;
  }

  public void cleanup() {
    Notifications.notifyCommitTransaction(morphline);
    Notifications.notifyShutdown(morphline);
  }

}
