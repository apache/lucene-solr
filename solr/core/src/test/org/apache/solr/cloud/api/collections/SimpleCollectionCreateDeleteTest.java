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
package org.apache.solr.cloud.api.collections;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.OverseerCollectionConfigSetProcessor;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TimeOut;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SimpleCollectionCreateDeleteTest extends AbstractFullDistribZkTestBase {

    public SimpleCollectionCreateDeleteTest() {
        sliceCount = 1;
    }

    @Test
    @ShardsFixed(num = 1)
    public void testCreateAndDeleteThenCreateAgain() throws Exception {
        String overseerNode = OverseerCollectionConfigSetProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
        String notOverseerNode = null;
        for (CloudJettyRunner cloudJetty : cloudJettys) {
            if (!overseerNode.equals(cloudJetty.nodeName)) {
                notOverseerNode = cloudJetty.nodeName;
                break;
            }
        }
        String collectionName = "SimpleCollectionCreateDeleteTest";
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 1)
                .setCreateNodeSet(overseerNode);

        NamedList<Object> request = create.process(cloudClient).getResponse();

        if (request.get("success") != null) {
            assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

            CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
            cloudClient.request(delete);

            assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

            // currently, removing a collection does not wait for cores to be unloaded
            TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
            while (true) {

                if (timeout.hasTimedOut()) {
                    throw new TimeoutException("Timed out waiting for all collections to be fully removed.");
                }

                boolean allContainersEmpty = true;
                for (JettySolrRunner jetty : jettys) {

                    Collection<SolrCore> cores = jetty.getCoreContainer().getCores();
                    for (SolrCore core : cores) {
                        CoreDescriptor cd = core.getCoreDescriptor();
                        if (cd != null) {
                            if (cd.getCloudDescriptor().getCollectionName().equals(collectionName)) {
                                allContainersEmpty = false;
                            }
                        }
                    }
                }
                if (allContainersEmpty) {
                    break;
                }
            }

            // create collection again on a node other than the overseer leader
            create = CollectionAdminRequest.createCollection(collectionName, 1, 1)
                    .setCreateNodeSet(notOverseerNode);
            request = create.process(cloudClient).getResponse();
            assertTrue("Collection creation should not have failed", request.get("success") != null);
        }
    }

    @Test
    @ShardsFixed(num = 1)
    public void testDeleteAlsoDeletesAutocreatedConfigSet() throws Exception {
        String collectionName = "SimpleCollectionCreateDeleteTest.testDeleteAlsoDeletesAutocreatedConfigSet";
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 1);

        NamedList<Object> request = create.process(cloudClient).getResponse();

        if (request.get("success") != null) {
            // collection exists now
            assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

            String configName = cloudClient.getZkStateReader().readConfigName(collectionName);

            // config for this collection is '.AUTOCREATED', and exists globally
            assertTrue(configName.endsWith(".AUTOCREATED"));
            assertTrue(cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));

            CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
            cloudClient.request(delete);

            // collection has been deleted
            assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
            // ... and so has its autocreated config set
            assertFalse("The auto-created config set should have been deleted with its collection", cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));
        }
    }

    @Test
    @ShardsFixed(num = 1)
    public void testDeleteDoesNotDeleteSharedAutocreatedConfigSet() throws Exception {
        String collectionNameInitial = "SimpleCollectionCreateDeleteTest.initialCollection";
        CollectionAdminRequest.Create createInitial = CollectionAdminRequest.createCollection(collectionNameInitial, 1, 1);

        NamedList<Object> requestInitial = createInitial.process(cloudClient).getResponse();

        if (requestInitial.get("success") != null) {
            // collection exists now
            assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionNameInitial, false));

            String configName = cloudClient.getZkStateReader().readConfigName(collectionNameInitial);

            // config for this collection is '.AUTOCREATED', and exists globally
            assertTrue(configName.endsWith(".AUTOCREATED"));
            assertTrue(cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));

            // create a second collection, sharing the same configSet
            String collectionNameWithSharedConfig = "SimpleCollectionCreateDeleteTest.collectionSharingAutocreatedConfigSet";
            CollectionAdminRequest.Create createWithSharedConfig = CollectionAdminRequest.createCollection(collectionNameWithSharedConfig, configName, 1, 1);

            NamedList<Object> requestWithSharedConfig = createWithSharedConfig.process(cloudClient).getResponse();
            assertTrue("The collection with shared config set should have been created", requestWithSharedConfig.get("success") != null);
            assertTrue("The new collection should exist after a successful creation", cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionNameWithSharedConfig, false));

            String configNameOfSecondCollection = cloudClient.getZkStateReader().readConfigName(collectionNameWithSharedConfig);

            assertEquals("Both collections should be using the same config", configName, configNameOfSecondCollection);

            // delete the initial collection - the config set should stay, since it is shared with the other collection
            CollectionAdminRequest.Delete deleteInitialCollection = CollectionAdminRequest.deleteCollection(collectionNameInitial);
            cloudClient.request(deleteInitialCollection);

            // initial collection has been deleted
            assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionNameInitial, false));
            // ... but not its autocreated config set, since it is shared with another collection
            assertTrue("The auto-created config set should NOT have been deleted. Another collection is using it.", cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));

            // delete the second collection - the config set should now be deleted, since it is no longer shared any other collection
            CollectionAdminRequest.Delete deleteSecondCollection = CollectionAdminRequest.deleteCollection(collectionNameWithSharedConfig);
            cloudClient.request(deleteSecondCollection);

            // the collection has been deleted
            assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionNameWithSharedConfig, false));
            // ... and the config set is now also deleted - once it doesn't get referenced by any collection
            assertFalse("The auto-created config set should have been deleted now. No collection is referencing it.", cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));
        }
    }
}
