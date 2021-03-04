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
package org.apache.solr.client.solrj.request.beans;

public class V2ApiConstants {
    private V2ApiConstants() { /* Private ctor prevents instantiation */ }

    /**
     * Parent key for collection or alias properties to set.
     */
    public static final String PROPERTIES_KEY = "properties";
    /**
     * Parent key for v2 params used to create a collection.
     */
    public static final String CREATE_COLLECTION_KEY = "create-collection";

    /**
     * Parent key holding alias-router parameters.
     */
    public static final String ROUTER_KEY = "router";

    /**
     * Parameter name for the configset used by a collection
     */
    public static final String CONFIG = "config";

    /**
     * Property controlling whether 'nodeSet' should be shuffled before use.
     */
    public static final String SHUFFLE_NODES = "shuffleNodes";

    /**
     * The set of nodes to consider as potential locations for a new collection or its constituent components.
     */
    public static final String NODE_SET = "nodeSet";

    /**
     * The collections to be included in an alias.
     */
    public static final String COLLECTIONS = "collections";
}
