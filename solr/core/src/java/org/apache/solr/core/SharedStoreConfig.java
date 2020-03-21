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
package org.apache.solr.core;

/**
 * Configuration class representing the configuration of Solr Cloud using shared storage
 * to persist index files. The configuration properties come from solr.xml and are loaded
 * via {@link SolrXmlConfig} at Solr startup. The presence of sharedStore section in the solr.xml
 * file of a Solr Cloud mode cluster with the config sharedStoreEnabled set to true indicates the 
 * user's intention to enable shared storage capabilities in the cluster and starts the necessary 
 * components required to create/support shared-type Solr collections.
 * 
 * TODO: This class is bare bones until we convert the many hard coded configuration values
 * into configurable fields that can be specified under this section. The current responsibility
 * of this class is to ensure shared storage is enabled on the cluster and initiate the necessary
 * processes associated with it.
 */
public class SharedStoreConfig {
}
