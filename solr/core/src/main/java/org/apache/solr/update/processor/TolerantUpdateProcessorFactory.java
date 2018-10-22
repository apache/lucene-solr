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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.util.plugin.SolrCoreAware;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * <p> 
 * Suppresses errors for individual add/delete commands within a single request.
 * Instead of failing on the first error, at most <code>maxErrors</code> errors (or unlimited 
 * if <code>-1==maxErrors</code>) are logged and recorded the batch continues. 
 * The client will receive a <code>status==200</code> response, which includes a list of errors 
 * that were tolerated.
 * </p>
 * <p>
 * If more then <code>maxErrors</code> occur, the first exception recorded will be re-thrown, 
 * Solr will respond with <code>status==5xx</code> or <code>status==4xx</code> 
 * (depending on the underlying exceptions) and it won't finish processing any more updates in the request. 
 * (ie: subsequent update commands in the request will not be processed even if they are valid).
 * </p>
 * 
 * <p>
 * <code>maxErrors</code> is an int value that can be specified in the configuration and/or overridden 
 * per request. If unset, it will default to {@link Integer#MAX_VALUE}.  Specifying an explicit value 
 * of <code>-1</code> is supported as shorthand for {@link Integer#MAX_VALUE}, all other negative 
 * integer values are not supported.
 * </p>
 * <p>
 * An example configuration would be:
 * </p>
 * <pre class="prettyprint">
 * &lt;updateRequestProcessorChain name="tolerant-chain"&gt;
 *   &lt;processor class="solr.TolerantUpdateProcessorFactory"&gt;
 *     &lt;int name="maxErrors"&gt;10&lt;/int&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessorChain&gt;
 * 
 * </pre>
 * 
 * <p>
 * The <code>maxErrors</code> parameter in the above chain could be overwritten per request, for example:
 * </p>
 * <pre class="prettyprint">
 * curl http://localhost:8983/update?update.chain=tolerant-chain&amp;maxErrors=100 -H "Content-Type: text/xml" -d @myfile.xml
 * </pre>
 * 
 * <p>
 * <b>NOTE:</b> The behavior of this UpdateProcessofFactory in conjunction with indexing operations 
 * while a Shard Split is actively in progress is not well defined (or sufficiently tested).  Users 
 * of this update processor are encouraged to either disable it, or pause updates, while any shard 
 * splitting is in progress (see <a href="https://issues.apache.org/jira/browse/SOLR-8881">SOLR-8881</a> 
 * for more details.)
 * </p>
 * @since 6.1.0
 */
public class TolerantUpdateProcessorFactory extends UpdateRequestProcessorFactory
  implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  
  /**
   * Parameter that defines how many errors the UpdateRequestProcessor will tolerate
   */
  private final static String MAX_ERRORS_PARAM = "maxErrors";
  
  /**
   * Default maxErrors value that will be use if the value is not set in configuration
   * or in the request
   */
  private int defaultMaxErrors = Integer.MAX_VALUE;

  private boolean informed = false;
  
  @SuppressWarnings("rawtypes")
  @Override
  public void init( NamedList args ) {

    Object maxErrorsObj = args.get(MAX_ERRORS_PARAM); 
    if (maxErrorsObj != null) {
      try {
        defaultMaxErrors = Integer.parseInt(maxErrorsObj.toString());
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unnable to parse maxErrors parameter: " + maxErrorsObj, e);
      }
      if (defaultMaxErrors < -1) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Config option '"+MAX_ERRORS_PARAM + "' must either be non-negative, or -1 to indicate 'unlimiited': " + maxErrorsObj.toString());
      }
    }
  }
  
  @Override
  public void inform(SolrCore core) {
    informed = true;
    if (null == core.getLatestSchema().getUniqueKeyField()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, this.getClass().getName() +
                              " requires a schema that includes a uniqueKey field.");
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {

    assert informed : "inform(SolrCore) never called?";
    
    // short circut if we're a replica processing commands from our leader
    DistribPhase distribPhase = DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    if (DistribPhase.FROMLEADER.equals(distribPhase)) {
      return next;
    }

    DistributedUpdateProcessorFactory.addParamToDistributedRequestWhitelist(req, MAX_ERRORS_PARAM);
    int maxErrors = req.getParams().getInt(MAX_ERRORS_PARAM, defaultMaxErrors);
    if (maxErrors < -1) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "'"+MAX_ERRORS_PARAM + "' must either be non-negative, or -1 to indicate 'unlimiited': " + maxErrors);
    }

    // NOTE: even if 0==maxErrors, we still inject processor into chain so responses has expected header info
    return new TolerantUpdateProcessor(req, rsp, next, maxErrors, distribPhase);
  }
}
