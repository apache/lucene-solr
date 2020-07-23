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

package org.apache.solr.cloud.gumi;

/**
 * Work order returned from the plugin to Solr to get a replica created.
 * Even though the interface is empty, it doesn't mean the implementation will be :-)
 * The plugin creates the instances using {@link WorkOrderFactory#createWorkOrderCreateReplica}
 * and does pass the required parameters. Adding getter methods here might be useful (for logging in the plugin, because
 * hopefully the implementation doesn't need to inspect the instances it created to know what to do with them....)
 * TODO I'll edit this comment at some point of course, it will hopefully not make it as is to the actual branch
 */
public interface CreateReplicaWorkOrder extends WorkOrder {
}
