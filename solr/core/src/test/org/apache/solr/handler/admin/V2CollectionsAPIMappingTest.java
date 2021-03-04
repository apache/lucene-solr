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
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.api.collections.CategoryRoutedAlias;
import org.apache.solr.cloud.api.collections.RoutedAlias;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.handler.CollectionsAPI;
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
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the API mappings found in {@link org.apache.solr.handler.CollectionsAPI}.
 *
 * This test bears many similarities to {@link TestCollectionAPIs} which appears to test the mappings indirectly by
 * checking message sent to the ZK overseer (which is similar, but not identical to the v1 param list).  If there's no
 * particular benefit to testing the mappings in this way (there very well may be), then we should combine these two
 * test classes at some point in the future using the simpler approach here.
 *
 * Note that the V2 requests made by these tests are not necessarily semantically valid.  They shouldn't be taken as
 * examples. In several instances, mutually exclusive JSON parameters are provided.  This is done to exercise conversion
 * of all parameters, even if particular combinations are never expected in the same request.
 */
public class V2CollectionsAPIMappingTest extends SolrTestCaseJ4 {

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
        final CollectionsAPI collectionsAPI = new CollectionsAPI(mockCollectionsHandler);
        apiBag.registerObject(collectionsAPI);
        apiBag.registerObject(collectionsAPI.collectionsCommands);
    }

    @Test
    public void testCreateCollectionAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections", "POST",
                "{'create': {" +
                        "'name': 'techproducts', " +
                        "'config':'_default', " +
                        "'router': {'name': 'composite', 'field': 'routeField', 'foo': 'bar'}, " +
                        "'shards': 'customShardName,anotherCustomShardName', " +
                        "'replicationFactor': 3," +
                        "'nrtReplicas': 1, " +
                        "'tlogReplicas': 1, " +
                        "'pullReplicas': 1, " +
                        "'nodeSet': ['localhost:8983_solr', 'localhost:7574_solr']," +
                        "'shuffleNodes': true," +
                        "'properties': {'foo': 'bar', 'foo2': 'bar2'}, " +
                        "'async': 'requestTrackingId', " +
                        "'waitForFinalState': false, " +
                        "'perReplicaState': false," +
                        "'numShards': 1}}");

        assertEquals(CollectionParams.CollectionAction.CREATE.lowerName, v1Params.get(ACTION));
        assertEquals("techproducts", v1Params.get(CommonParams.NAME));
        assertEquals("_default", v1Params.get(CollectionAdminParams.COLL_CONF));
        assertEquals("composite", v1Params.get("router.name"));
        assertEquals("routeField", v1Params.get("router.field"));
        assertEquals("bar", v1Params.get("router.foo"));
        assertEquals("customShardName,anotherCustomShardName", v1Params.get(ShardParams.SHARDS));
        assertEquals(3, v1Params.getPrimitiveInt(ZkStateReader.REPLICATION_FACTOR));
        assertEquals(1, v1Params.getPrimitiveInt(ZkStateReader.NRT_REPLICAS));
        assertEquals(1, v1Params.getPrimitiveInt(ZkStateReader.TLOG_REPLICAS));
        assertEquals(1, v1Params.getPrimitiveInt(ZkStateReader.PULL_REPLICAS));
        assertEquals("localhost:8983_solr,localhost:7574_solr", v1Params.get(CollectionAdminParams.CREATE_NODE_SET_PARAM));
        assertEquals(true, v1Params.getPrimitiveBool(CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM));
        assertEquals("bar", v1Params.get("property.foo"));
        assertEquals("bar2", v1Params.get("property.foo2"));
        assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
        assertEquals(false, v1Params.getPrimitiveBool(CommonAdminParams.WAIT_FOR_FINAL_STATE));
        assertEquals(false, v1Params.getPrimitiveBool(DocCollection.PER_REPLICA_STATE));
        assertEquals(1, v1Params.getPrimitiveInt(CollectionAdminParams.NUM_SHARDS));
    }

    @Test
    public void testCreateAliasAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections", "POST",
                "{'create-alias': {" +
                        "'name': 'aliasName', " +
                        "'collections': ['techproducts1', 'techproducts2'], " +
                        "'tz': 'someTimeZone', " +
                        "'async': 'requestTrackingId', " +
                        "'router': {" +
                        "    'name': 'time', " +
                        "    'field': 'date_dt', " +
                        "    'interval': '+1HOUR', " +
                        "     'maxFutureMs': 3600, " +
                        "     'preemptiveCreateMath': 'somePreemptiveCreateMathString', " +
                        "     'autoDeleteAge': 'someAutoDeleteAgeExpression', " +
                        "     'maxCardinality': 36, " +
                        "     'mustMatch': 'someRegex', " +
                        "}, " +
                        "'create-collection': {" +
                        "     'numShards': 1, " +
                        "     'properties': {'foo': 'bar', 'foo2': 'bar2'}, " +
                        "     'replicationFactor': 3 " +
                        "}" +
                        "}}");

        assertEquals(CollectionParams.CollectionAction.CREATEALIAS.lowerName, v1Params.get(ACTION));
        assertEquals("aliasName", v1Params.get(CommonParams.NAME));
        assertEquals("techproducts1,techproducts2", v1Params.get("collections"));
        assertEquals("someTimeZone", v1Params.get(CommonParams.TZ.toLowerCase(Locale.ROOT)));
        assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
        assertEquals("time", v1Params.get(CollectionAdminRequest.CreateTimeRoutedAlias.ROUTER_TYPE_NAME));
        assertEquals("date_dt", v1Params.get(CollectionAdminRequest.CreateTimeRoutedAlias.ROUTER_FIELD));
        assertEquals("+1HOUR", v1Params.get(CollectionAdminRequest.CreateTimeRoutedAlias.ROUTER_INTERVAL));
        assertEquals(3600, v1Params.getPrimitiveInt(CollectionAdminRequest.CreateTimeRoutedAlias.ROUTER_MAX_FUTURE));
        assertEquals("somePreemptiveCreateMathString", v1Params.get(CollectionAdminRequest.CreateTimeRoutedAlias.ROUTER_PREEMPTIVE_CREATE_WINDOW));
        assertEquals("someAutoDeleteAgeExpression", v1Params.get(CollectionAdminRequest.CreateTimeRoutedAlias.ROUTER_AUTO_DELETE_AGE));
        assertEquals(36, v1Params.getPrimitiveInt(CategoryRoutedAlias.ROUTER_MAX_CARDINALITY));
        assertEquals("someRegex", v1Params.get(CategoryRoutedAlias.ROUTER_MUST_MATCH));
        assertEquals(1, v1Params.getPrimitiveInt(RoutedAlias.CREATE_COLLECTION_PREFIX + CollectionAdminParams.NUM_SHARDS));
        assertEquals("bar", v1Params.get(RoutedAlias.CREATE_COLLECTION_PREFIX + "property.foo"));
        assertEquals("bar2", v1Params.get(RoutedAlias.CREATE_COLLECTION_PREFIX + "property.foo2"));
        assertEquals(3, v1Params.getPrimitiveInt(RoutedAlias.CREATE_COLLECTION_PREFIX + ZkStateReader.REPLICATION_FACTOR));
    }

    @Test
    public void testDeleteAliasAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections", "POST",
                "{'delete-alias': {" +
                        "'name': 'aliasName', " +
                        "'async': 'requestTrackingId'" +
                        "}}");

        assertEquals(CollectionParams.CollectionAction.DELETEALIAS.lowerName, v1Params.get(ACTION));
        assertEquals("aliasName", v1Params.get(CommonParams.NAME));
        assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
    }

    @Test
    public void testSetAliasAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections", "POST",
                "{'set-alias-property': {" +
                        "'name': 'aliasName', " +
                        "'async': 'requestTrackingId', " +
                        "'properties': {'foo':'bar', 'foo2':'bar2'}" +
                        "}}");

        assertEquals(CollectionParams.CollectionAction.ALIASPROP.lowerName, v1Params.get(ACTION));
        assertEquals("aliasName", v1Params.get(CommonParams.NAME));
        assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
        assertEquals("bar", v1Params.get("property.foo"));
        assertEquals("bar2", v1Params.get("property.foo2"));
    }

    @Test
    public void testBackupAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections", "POST",
                "{'backup-collection': {" +
                        "'name': 'backupName', " +
                        "'collection': 'collectionName', " +
                        "'location': '/some/location/uri', " +
                        "'repository': 'someRepository', " +
                        "'followAliases': true, " +
                        "'indexBackup': 'copy-files', " +
                        "'commitName': 'someSnapshotName', " +
                        "'incremental': true, " +
                        "'async': 'requestTrackingId' " +
                        "}}");

        assertEquals(CollectionParams.CollectionAction.BACKUP.lowerName, v1Params.get(ACTION));
        assertEquals("backupName", v1Params.get(CommonParams.NAME));
        assertEquals("collectionName", v1Params.get(BackupManager.COLLECTION_NAME_PROP));
        assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
        assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
        assertEquals(true, v1Params.getPrimitiveBool(CollectionAdminParams.FOLLOW_ALIASES));
        assertEquals("copy-files", v1Params.get(CollectionAdminParams.INDEX_BACKUP_STRATEGY));
        assertEquals("someSnapshotName", v1Params.get(CoreAdminParams.COMMIT_NAME));
        assertEquals(true, v1Params.getPrimitiveBool(CoreAdminParams.BACKUP_INCREMENTAL));
        assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
    }

    @Test
    public void testRestoreAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections", "POST",
                "{'restore-collection': {" +
                        "'name': 'backupName', " +
                        "'collection': 'collectionName', " +
                        "'location': '/some/location/uri', " +
                        "'repository': 'someRepository', " +
                        "'backupId': 123, " +
                        "'async': 'requestTrackingId', " +
                        "'create-collection': {" +
                        "     'numShards': 1, " +
                        "     'properties': {'foo': 'bar', 'foo2': 'bar2'}, " +
                        "     'replicationFactor': 3 " +
                        "}" +
                        "}}");

        assertEquals(CollectionParams.CollectionAction.RESTORE.lowerName, v1Params.get(ACTION));
        assertEquals("backupName", v1Params.get(CommonParams.NAME));
        assertEquals("collectionName", v1Params.get(BackupManager.COLLECTION_NAME_PROP));
        assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
        assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
        assertEquals(123, v1Params.getPrimitiveInt(CoreAdminParams.BACKUP_ID));
        assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
        // NOTE: Unlike other v2 APIs that have a nested object for collection-creation params, restore's v1 equivalent
        // for these properties doesn't have a "create-collection." prefix.
        assertEquals(1, v1Params.getPrimitiveInt(CollectionAdminParams.NUM_SHARDS));
        assertEquals("bar", v1Params.get("property.foo"));
        assertEquals("bar2", v1Params.get("property.foo2"));
        assertEquals(3, v1Params.getPrimitiveInt(ZkStateReader.REPLICATION_FACTOR));
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
