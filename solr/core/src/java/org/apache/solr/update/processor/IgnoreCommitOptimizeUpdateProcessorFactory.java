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
package org.apache.solr.update.processor;

import static org.apache.solr.common.SolrException.ErrorCode;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Gives system administrators a way to ignore explicit commit or optimize requests from clients.
 * The factory can be configured to return a specific HTTP response code, default is 403, and
 * optional response message, such as to warn the client application that its request was ignored.
 * </p>
 * @since 5.0.0
 */
public class IgnoreCommitOptimizeUpdateProcessorFactory extends UpdateRequestProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEFAULT_RESPONSE_MSG = "Explicit commit/optimize requests are forbidden!";
  
  protected ErrorCode errorCode;
  protected String responseMsg;
  protected boolean ignoreOptimizeOnly = false; // default behavior is to ignore commits and optimize

  @Override
  public void init(@SuppressWarnings({"rawtypes"})final NamedList args) {
    SolrParams params = (args != null) ? args.toSolrParams() : null;
    if (params == null) {
      errorCode = ErrorCode.FORBIDDEN; // default is 403 error
      responseMsg = DEFAULT_RESPONSE_MSG;
      ignoreOptimizeOnly = false;
      return;
    }

    ignoreOptimizeOnly = params.getBool("ignoreOptimizeOnly", false);

    int statusCode = params.getInt("statusCode", ErrorCode.FORBIDDEN.code);
    if (statusCode == 200) {
      errorCode = null; // not needed but makes the logic clearer
      responseMsg = params.get("responseMessage"); // OK to be null for 200's
    } else {
      errorCode = ErrorCode.getErrorCode(statusCode);
      if (errorCode == ErrorCode.UNKNOWN) {
        // only allow the error codes supported by the SolrException.ErrorCode class
        StringBuilder validCodes = new StringBuilder();
        int appended = 0;
        for (ErrorCode code : ErrorCode.values()) {
          if (code != ErrorCode.UNKNOWN) {
            if (appended++ > 0) validCodes.append(", ");
            validCodes.append(code.code);
          }
        }
        throw new IllegalArgumentException("Configured status code " + statusCode +
            " not supported! Please choose one of: " + validCodes.toString());
      }

      // must always have a response message if sending an error code
      responseMsg = params.get("responseMessage", DEFAULT_RESPONSE_MSG);
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new IgnoreCommitOptimizeUpdateProcessor(rsp, this, next);
  }
  
  static class IgnoreCommitOptimizeUpdateProcessor extends UpdateRequestProcessor {

    private final SolrQueryResponse rsp;
    private final ErrorCode errorCode;
    private final String responseMsg;
    private final boolean ignoreOptimizeOnly;

    IgnoreCommitOptimizeUpdateProcessor(SolrQueryResponse rsp,
                                        IgnoreCommitOptimizeUpdateProcessorFactory factory,
                                        UpdateRequestProcessor next)
    {
      super(next);
      this.rsp = rsp;
      this.errorCode = factory.errorCode;
      this.responseMsg = factory.responseMsg;
      this.ignoreOptimizeOnly = factory.ignoreOptimizeOnly;
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {

      if (ignoreOptimizeOnly && !cmd.optimize) {
        // we're setup to only ignore optimize requests so it's OK to pass this commit on down the line
        if (next != null) next.processCommit(cmd);
        return;
      }

      if (cmd.getReq().getParams().getBool(DistributedUpdateProcessor.COMMIT_END_POINT, false)) {
        // this is a targeted commit from replica to leader needed for recovery, so can't be ignored
        if (next != null) next.processCommit(cmd);
        return;
      }

      final String cmdType = cmd.optimize ? "optimize" : "commit";
      if (errorCode != null) {
        IgnoreCommitOptimizeUpdateProcessorFactory.log.info(
            "{} from client application ignored with error code: {}", cmdType, errorCode.code);
        rsp.setException(new SolrException(errorCode, responseMsg));
      } else {
        // errorcode is null, treat as a success with an optional message warning the commit request was ignored
        IgnoreCommitOptimizeUpdateProcessorFactory.log.info(
            "{} from client application ignored with status code: 200", cmdType);
        if (responseMsg != null) {
          NamedList<Object> responseHeader = rsp.getResponseHeader();
          if (responseHeader != null) {
            responseHeader.add("msg", responseMsg);
          } else {
            responseHeader = new SimpleOrderedMap<Object>();
            responseHeader.add("msg", responseMsg);
            rsp.addResponseHeader(responseHeader);
          }
        }
      }
    }
  }
}
