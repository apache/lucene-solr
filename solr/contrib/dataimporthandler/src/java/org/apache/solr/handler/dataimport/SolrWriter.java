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
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Set;

/**
 * <p> Writes documents to SOLR. </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class SolrWriter extends DIHWriterBase implements DIHWriter {
  private static final Logger log = LoggerFactory.getLogger(SolrWriter.class);

  static final String LAST_INDEX_KEY = "last_index_time";

  private final UpdateRequestProcessor processor;

  public SolrWriter(UpdateRequestProcessor processor) {
    this.processor = processor;
  }

  public void close() {
  	try {
  		processor.finish();
  	} catch (IOException e) {
  		throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
  				"Unable to call finish() on UpdateRequestProcessor", e);
  	}
  }
  public boolean upload(SolrInputDocument d) {
    try {
      AddUpdateCommand command = new AddUpdateCommand();
      command.solrDoc = d;
      command.allowDups = false;
      command.overwritePending = true;
      command.overwriteCommitted = true;
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
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
      delCmd.id = id.toString();
      delCmd.fromPending = true;
      delCmd.fromCommitted = true;
      processor.processDelete(delCmd);
    } catch (IOException e) {
      log.error("Exception while deleteing: " + id, e);
    }
  }
  	
	public void deleteByQuery(String query) {
    try {
      log.info("Deleting documents from Solr with query: " + query);
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
      delCmd.query = query;
      delCmd.fromCommitted = true;
      delCmd.fromPending = true;
      processor.processDelete(delCmd);
    } catch (IOException e) {
      log.error("Exception while deleting by query: " + query, e);
    }
  }

	public void commit(boolean optimize) {
    try {
      CommitUpdateCommand commit = new CommitUpdateCommand(optimize);
      processor.processCommit(commit);
    } catch (Throwable t) {
      log.error("Exception while solr commit.", t);
    }
  }

	public void rollback() {
    try {
      RollbackUpdateCommand rollback = new RollbackUpdateCommand();
      processor.processRollback(rollback);
    } catch (Throwable t) {
      log.error("Exception while solr rollback.", t);
    }
  }

	public void doDeleteAll() {
    try {
      DeleteUpdateCommand deleteCommand = new DeleteUpdateCommand();
      deleteCommand.query = "*:*";
      deleteCommand.fromCommitted = true;
      deleteCommand.fromPending = true;
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
	public void init(Context context) {
		/* NO-OP */		
	}	
}
