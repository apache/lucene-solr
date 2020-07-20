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

import java.util.Optional;

/**
 * A fully specified work order for placement, deletion or move to be applied to the cluster.<p>
 * Fully specified is to be specified... but likely means the actual {@link Node} on which to do the placement has been
 * decided.
 */
public interface WorkOrder {
  /**
   * <p>When it makes sense (it might not for "maintenance" {@link WorkOrder}'s for example not directly linked to a specific
   * {@link Request}), returns the {@link Request} that triggered the creation of this {@link WorkOrder}.</p>
   * TODO Decide if this is really needed, because if it's optional it might better be removed
   */
  Optional<Request> getCorrespondingRequest();
}
