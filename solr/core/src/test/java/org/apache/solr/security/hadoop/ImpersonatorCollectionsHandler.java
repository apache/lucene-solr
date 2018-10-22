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
package org.apache.solr.security.hadoop;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.HadoopAuthPlugin;
import org.apache.solr.security.KerberosPlugin;
import org.junit.Assert;

/**
 * This class extends {@linkplain CollectionsHandler} and implements extra validations
 * for verifying proxy users support in {@linkplain HadoopAuthPlugin}
 */
public class ImpersonatorCollectionsHandler extends CollectionsHandler {
  static AtomicBoolean called = new AtomicBoolean(false);

  public ImpersonatorCollectionsHandler() {
    super();
  }

  public ImpersonatorCollectionsHandler(final CoreContainer coreContainer) {
    super(coreContainer);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    called.set(true);
    super.handleRequestBody(req, rsp);
    String doAs = req.getParams().get(KerberosPlugin.IMPERSONATOR_DO_AS_HTTP_PARAM);
    if (doAs != null) {
      HttpServletRequest httpRequest = (HttpServletRequest)req.getContext().get("httpRequest");
      Assert.assertNotNull(httpRequest);
      String user = req.getParams().get(PseudoAuthenticator.USER_NAME);
      Assert.assertNotNull(user);
      Assert.assertEquals(user, httpRequest.getAttribute(KerberosPlugin.IMPERSONATOR_USER_NAME));
    }
  }
}
