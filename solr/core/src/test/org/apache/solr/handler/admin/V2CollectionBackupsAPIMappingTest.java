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

import com.google.common.collect.Maps;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.CollectionBackupsAPI;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class V2CollectionBackupsAPIMappingTest extends SolrTestCaseJ4 {

  private ApiBag apiBag;
  private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
  private CollectionsHandler mockCollectionsHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() throws Exception {
    mockCollectionsHandler = mock(CollectionsHandler.class);
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    apiBag = new ApiBag(false);
    final CollectionBackupsAPI collBackupsAPI = new CollectionBackupsAPI(mockCollectionsHandler);
    apiBag.registerObject(collBackupsAPI);
  }

  @Test
  public void testDeleteBackupsAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/backups", "POST",
            "{'delete-backups': {" +
                    "'name': 'backupName', " +
                    "'location': '/some/location/uri', " +
                    "'repository': 'someRepository', " +
                    "'backupId': 123, " +
                    "'maxNumBackupPoints': 456, " +
                    "'purgeUnused': true, " +
                    "'async': 'requestTrackingId'" +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.DELETEBACKUP.lowerName, v1Params.get(ACTION));
    assertEquals("backupName", v1Params.get(NAME));
    assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
    assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
    assertEquals(123, v1Params.getPrimitiveInt(CoreAdminParams.BACKUP_ID));
    assertEquals(456, v1Params.getPrimitiveInt(CoreAdminParams.MAX_NUM_BACKUP_POINTS));
    assertEquals(true, v1Params.getPrimitiveBool(CoreAdminParams.BACKUP_PURGE_UNUSED));
    assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
  }

  @Test
  public void testListBackupsAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/backups", "POST",
            "{'list-backups': {" +
                    "'name': 'backupName', " +
                    "'location': '/some/location/uri', " +
                    "'repository': 'someRepository' " +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.LISTBACKUP.lowerName, v1Params.get(ACTION));
    assertEquals("backupName", v1Params.get(NAME));
    assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
    assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
  }

  private SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, Maps.newHashMap()) {
      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        if (v2RequestBody == null) return Collections.emptyList();
        return ApiBag.getCommandOperations(new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
      }

      @Override
      public Map<String, String> getPathTemplateValues() {
        return parts;
      }

      @Override
      public String getHttpMethod() {
        return method;
      }
    };


    api.call(req, rsp);
    verify(mockCollectionsHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue().getParams();
  }
}
