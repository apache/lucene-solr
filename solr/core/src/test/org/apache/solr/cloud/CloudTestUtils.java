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

package org.apache.solr.cloud;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.RequestWriter.StringPayloadContentWriter;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.TimeOut;
import org.junit.Assert;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;


/**
 * Some useful methods for SolrCloud tests.
 */
public class CloudTestUtils {

  public static final int DEFAULT_TIMEOUT = 90;

  /**
   * Wait for a particular named trigger to be scheduled.
   * <p>
   * This is a convenience method that polls the autoscaling API looking for a trigger with the 
   * specified name using the {@link #DEFAULT_TIMEOUT}.  It is particularly useful for tests 
   * that want to know when the Overseer has finished scheduling the automatic triggers on startup.
   * </p>
   *
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param triggerName the name of the trigger we need to see sheduled in order to return successfully
   * @see #suspendTrigger
   */
  public static long waitForTriggerToBeScheduled(final SolrCloudManager cloudManager,
                                                 final String triggerName)
    throws InterruptedException, TimeoutException, IOException {

    TimeOut timeout = new TimeOut(DEFAULT_TIMEOUT, TimeUnit.SECONDS, cloudManager.getTimeSource());
    while (!timeout.hasTimedOut()) {
      final SolrResponse response = cloudManager.request(AutoScalingRequest.create(SolrRequest.METHOD.GET, null));
      final Map<String,?> triggers = (Map<String,?>) response.getResponse().get("triggers");
      Assert.assertNotNull("null triggers in response from autoscaling request", triggers);
      
      if ( triggers.containsKey(triggerName) ) {
        return timeout.timeElapsed(TimeUnit.MILLISECONDS);
      }
      timeout.sleep(100);
    }
    throw new TimeoutException("Never saw trigger with name: " + triggerName);
  }

  /**
   * Suspends the trigger with the specified name
   * <p>
   * This is a convenience method that sends a <code>suspend-trigger</code> command to the autoscaling
   * API for the specified trigger.  It is particularly useful for tests that may need to disable automatic
   * triggers such as <code>.scheduled_maintenance</code> in order to test their own
   * triggers.
   * </p>
   *
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param triggerName the name of the trigger to suspend.  This must already be scheduled.
   * @see #assertAutoScalingRequest
   * @see #waitForTriggerToBeScheduled
   */
  public static void suspendTrigger(final SolrCloudManager cloudManager,
                                    final String triggerName) throws IOException {
    assertAutoScalingRequest(cloudManager, "{'suspend-trigger' : {'name' : '"+triggerName+"'} }");
  }

  /**
   * Creates &amp; executes an autoscaling request against the current cluster, asserting that 
   * the result is a success.
   * 
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param json The request to POST to the AutoScaling Handler
   * @see AutoScalingRequest#create
   */
  public static void assertAutoScalingRequest(final SolrCloudManager cloudManager,
                                              final String json) throws IOException {
    // TODO: a lot of code that directly uses AutoScalingRequest.create should use this method
    
    final SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, json);
    final SolrResponse rsp = cloudManager.request(req);
    final String result = rsp.getResponse().get("result").toString();
    Assert.assertEquals("Unexpected result from auto-scaling command: " + json + " -> " + rsp,
                        "success", result);
  }

  
  /**
   * Helper class for sending (JSON) autoscaling requests that can randomize between V1 and V2 requests
   */
  public static class AutoScalingRequest extends SolrRequest {
    private SolrParams params = null;
    /**
     * Creates a request using a randomized root path (V1 vs V2)
     *
     * @param m HTTP Method to use
     * @aram message JSON payload, may be null
     */
    public static SolrRequest create(SolrRequest.METHOD m, String message) {
      return create(m, null, message);
    }
    /**
     * Creates a request using a randomized root path (V1 vs V2)
     *
     * @param m HTTP Method to use
     * @param subPath optional sub-path under <code>"$ROOT/autoscaling"</code>. may be null, 
     *        otherwise must start with "/"
     * @param message JSON payload, may be null
     */
    public static SolrRequest create(SolrRequest.METHOD m, String subPath, String message) {
      return create(m,subPath,message,null);

    }
    public static SolrRequest create(SolrRequest.METHOD m, String subPath, String message, SolrParams params) {
      final boolean useV1 = LuceneTestCase.random().nextBoolean();
      String path = useV1 ? "/admin/autoscaling" : "/cluster/autoscaling";
      if (null != subPath) {
        assert subPath.startsWith("/");
        path += subPath;
      }
      return useV1
          ? new AutoScalingRequest(m, path, message).withParams(params)
          : new V2Request.Builder(path).withMethod(m).withParams(params).withPayload(message).build();

    }

    protected final String message;

    /**
     * Simple request
     * @param m HTTP Method to use
     * @param path path to send request to
     * @param message JSON payload, may be null
     */
    private AutoScalingRequest(METHOD m, String path, String message) {
      super(m, path);
      this.message = message;
    }


    AutoScalingRequest withParams(SolrParams params){
      this.params = params;
      return this;
    }
    @Override
    public SolrParams getParams() {
      return params;
    }

    @Override
    public RequestWriter.ContentWriter getContentWriter(String expectedType) {
      return message == null ? null : new StringPayloadContentWriter(message, JSON_MIME);
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return new SolrResponseBase();
    }
  }
}
