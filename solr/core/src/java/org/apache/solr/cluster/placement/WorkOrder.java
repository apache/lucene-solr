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

package org.apache.solr.cluster.placement;

/**
 * A fully specified work order for placement, deletion or move to be applied to the cluster.<p>
 * Fully specified means the actual {@link Node}'s on which to place replicas have been decided.
 *
 * Instances are created by plugin code using {@link WorkOrderFactory}. This interface obviously doesn't expose much but
 * the underlying Solr side implementation has all that is needed (and will do at least one cast in order to execute the
 * work order, likely then using some type of visitor pattern).
 */
public interface WorkOrder {
}
