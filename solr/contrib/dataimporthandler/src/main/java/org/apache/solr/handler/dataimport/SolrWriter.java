/**
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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * <p> Writes documents to SOLR as well as provides methods for loading and persisting last index time. </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class SolrWriter {
  private static final Logger log = LoggerFactory.getLogger(SolrWriter.class);

  static final String IMPORTER_PROPERTIES = "dataimport.properties";

  static final String LAST_INDEX_KEY = "last_index_time";

  private final UpdateRequestProcessor processor;

  private final String configDir;

  private String persistFilename = IMPORTER_PROPERTIES;

  DebugLogger debugLogger;

  SolrQueryRequest req;

  public SolrWriter(UpdateRequestProcessor processor, String confDir, SolrQueryRequest req) {
    this.processor = processor;
    configDir = confDir;
    this.req = req;
  }
  public SolrWriter(UpdateRequestProcessor processor, String confDir, String filePrefix, SolrQueryRequest req) {
    this.processor = processor;
    configDir = confDir;
    if(filePrefix != null){
      persistFilename = filePrefix+".properties";
    }
    this.req = req;
  }

  public boolean upload(SolrInputDocument d) {
    try {
      AddUpdateCommand command = new AddUpdateCommand(req);
      command.solrDoc = d;
      processor.processAdd(command);
    } catch (Exception e) {
      log.warn("Error creating document : " + d, e);
      return false;
    }

    return true;
  }

  public void deleteDoc(Object id) {
    try {
      log.info("Deleting document: " + id);
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand(req);
      delCmd.id = id.toString();
      processor.processDelete(delCmd);
    } catch (IOException e) {
      log.error("Exception while deleteing: " + id, e);
    }
  }


  void persist(Properties p) {
    OutputStream propOutput = null;

    Properties props = readIndexerProperties();

    try {
      props.putAll(p);
      String filePath = configDir;
      if (configDir != null && !configDir.endsWith(File.separator))
        filePath += File.separator;
      filePath += persistFilename;
      propOutput = new FileOutputStream(filePath);
      props.store(propOutput, null);
      log.info("Wrote last indexed time to " + persistFilename);
    } catch (FileNotFoundException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Unable to persist Index Start Time", e);
    } catch (IOException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Unable to persist Index Start Time", e);
    } finally {
      try {
        if (propOutput != null)
          propOutput.close();
      } catch (IOException e) {
        propOutput = null;
      }
    }
  }

  void finish() {
    try {
      processor.finish();
    } catch (IOException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "Unable to call finish() on UpdateRequestProcessor", e);
    }
  }
  
  Properties readIndexerProperties() {
    Properties props = new Properties();
    InputStream propInput = null;

    try {
      propInput = new FileInputStream(configDir
              + persistFilename);
      props.load(propInput);
      log.info("Read " + persistFilename);
    } catch (Exception e) {
      log.warn("Unable to read: " + persistFilename);
    } finally {
      try {
        if (propInput != null)
          propInput.close();
      } catch (IOException e) {
        propInput = null;
      }
    }

    return props;
  }

  public void deleteByQuery(String query) {
    try {
      log.info("Deleting documents from Solr with query: " + query);
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand(req);
      delCmd.query = query;
      processor.processDelete(delCmd);
    } catch (IOException e) {
      log.error("Exception while deleting by query: " + query, e);
    }
  }

  public void commit(boolean optimize) {
    try {
      CommitUpdateCommand commit = new CommitUpdateCommand(req,optimize);
      processor.processCommit(commit);
    } catch (Throwable t) {
      log.error("Exception while solr commit.", t);
    }
  }

  public void rollback() {
    try {
      RollbackUpdateCommand rollback = new RollbackUpdateCommand(req);
      processor.processRollback(rollback);
    } catch (Throwable t) {
      log.error("Exception while solr rollback.", t);
    }
  }

  public void doDeleteAll() {
    try {
      DeleteUpdateCommand deleteCommand = new DeleteUpdateCommand(req);
      deleteCommand.query = "*:*";
      processor.processDelete(deleteCommand);
    } catch (IOException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Exception in full dump while deleting all documents.", e);
    }
  }

  static String getResourceAsString(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    byte[] buf = new byte[1024];
    int sz = 0;
    try {
      while ((sz = in.read(buf)) != -1) {
        baos.write(buf, 0, sz);
      }
    } finally {
      try {
        in.close();
      } catch (Exception e) {

      }
    }
    return new String(baos.toByteArray(), "UTF-8");
  }

  static String getDocCount() {
    if (DocBuilder.INSTANCE.get() != null) {
      return ""
              + (DocBuilder.INSTANCE.get().importStatistics.docCount.get() + 1);
    } else {
      return null;
    }
  }

  public DebugLogger getDebugLogger() {
    if (debugLogger == null) {
      debugLogger = new DebugLogger(this);
    }
    return debugLogger;
  }

  /**
   * This method is used for verbose debugging
   *
   * @param event The event name start.entity ,end.entity ,transformer.row
   * @param name  Name of the entity/transformer
   * @param row   The actual data . Can be a Map<String,object> or a List<Map<String,object>>
   */
  public void log(int event, String name, Object row) {
    getDebugLogger().log(event, name, row);
  }

  public static final int START_ENTITY = 1, END_ENTITY = 2,
          TRANSFORMED_ROW = 3, ENTITY_META = 4, PRE_TRANSFORMER_ROW = 5,
          START_DOC = 6, END_DOC = 7, ENTITY_OUT = 8, ROW_END = 9,
          TRANSFORMER_EXCEPTION = 10, ENTITY_EXCEPTION = 11, DISABLE_LOGGING = 12,
          ENABLE_LOGGING = 13;
}
