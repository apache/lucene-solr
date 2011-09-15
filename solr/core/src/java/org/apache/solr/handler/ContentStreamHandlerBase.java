package org.apache.solr.handler;
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Shares common code between various handlers that manipulate {@link org.apache.solr.common.util.ContentStream} objects.
 *
 **/
public abstract class ContentStreamHandlerBase extends RequestHandlerBase {
  public static Logger log = LoggerFactory.getLogger(XmlUpdateRequestHandler.class);

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    UpdateRequestProcessorChain processorChain =
            req.getCore().getUpdateProcessingChain(SolrPluginUtils.resolveUpdateChainParam(params, log));

    UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);

    try {
      ContentStreamLoader documentLoader = newLoader(req, processor);


      Iterable<ContentStream> streams = req.getContentStreams();
      if (streams == null) {
        if (!RequestHandlerUtils.handleCommit(req, processor, params, false) && !RequestHandlerUtils.handleRollback(req, processor, params, false)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing content stream");
        }
      } else {

        for (ContentStream stream : streams) {
          documentLoader.load(req, rsp, stream);
        }

        // Perhaps commit from the parameters
        RequestHandlerUtils.handleCommit(req, processor, params, false);
        RequestHandlerUtils.handleRollback(req, processor, params, false);
      }
    } finally {
      // finish the request
      processor.finish();
    }
  }

  protected abstract ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor);
}
