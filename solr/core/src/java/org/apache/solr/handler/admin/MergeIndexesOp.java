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

package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class MergeIndexesOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    SolrCore core = it.handler.coreContainer.getCore(cname);
    SolrQueryRequest wrappedReq = null;
    if (core == null) return;

    List<SolrCore> sourceCores = Lists.newArrayList();
    List<RefCounted<SolrIndexSearcher>> searchers = Lists.newArrayList();
    // stores readers created from indexDir param values
    List<DirectoryReader> readersToBeClosed = Lists.newArrayList();
    Map<Directory, Boolean> dirsToBeReleased = new HashMap<>();

    try {
      String[] dirNames = params.getParams(CoreAdminParams.INDEX_DIR);
      if (dirNames == null || dirNames.length == 0) {
        String[] sources = params.getParams("srcCore");
        if (sources == null || sources.length == 0)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "At least one indexDir or srcCore must be specified");

        for (int i = 0; i < sources.length; i++) {
          String source = sources[i];
          SolrCore srcCore = it.handler.coreContainer.getCore(source);
          if (srcCore == null)
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Core: " + source + " does not exist");
          sourceCores.add(srcCore);
        }
      } else {
        // Validate each 'indexDir' input as valid
        Arrays.stream(dirNames).forEach(indexDir -> core.getCoreContainer().assertPathAllowed(Paths.get(indexDir)));
        DirectoryFactory dirFactory = core.getDirectoryFactory();
        for (int i = 0; i < dirNames.length; i++) {
          boolean markAsDone = false;
          if (dirFactory instanceof CachingDirectoryFactory) {
            if (!((CachingDirectoryFactory) dirFactory).getLivePaths().contains(dirNames[i])) {
              markAsDone = true;
            }
          }
          Directory dir = dirFactory.get(dirNames[i], DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
          dirsToBeReleased.put(dir, markAsDone);
          // TODO: why doesn't this use the IR factory? what is going on here?
          readersToBeClosed.add(DirectoryReader.open(dir));
        }
      }

      List<DirectoryReader> readers = null;
      if (readersToBeClosed.size() > 0) {
        readers = readersToBeClosed;
      } else {
        readers = Lists.newArrayList();
        for (SolrCore solrCore : sourceCores) {
          // record the searchers so that we can decref
          RefCounted<SolrIndexSearcher> searcher = solrCore.getSearcher();
          searchers.add(searcher);
          readers.add(searcher.get().getIndexReader());
        }
      }

      UpdateRequestProcessorChain processorChain =
          core.getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));
      wrappedReq = new LocalSolrQueryRequest(core, it.req.getParams());
      UpdateRequestProcessor processor =
          processorChain.createProcessor(wrappedReq, it.rsp);
      processor.processMergeIndexes(new MergeIndexesCommand(readers, it.req));
    } catch (Exception e) {
      // log and rethrow so that if the finally fails we don't lose the original problem
      log.error("ERROR executing merge:", e);
      throw e;
    } finally {
      for (RefCounted<SolrIndexSearcher> searcher : searchers) {
        if (searcher != null) searcher.decref();
      }
      for (SolrCore solrCore : sourceCores) {
        if (solrCore != null) solrCore.close();
      }
      IOUtils.closeWhileHandlingException(readersToBeClosed);
      Set<Map.Entry<Directory, Boolean>> entries = dirsToBeReleased.entrySet();
      for (Map.Entry<Directory, Boolean> entry : entries) {
        DirectoryFactory dirFactory = core.getDirectoryFactory();
        Directory dir = entry.getKey();
        boolean markAsDone = entry.getValue();
        if (markAsDone) {
          dirFactory.doneWithDirectory(dir);
        }
        dirFactory.release(dir);
      }
      if (wrappedReq != null) wrappedReq.close();
      core.close();
    }
  }
}
