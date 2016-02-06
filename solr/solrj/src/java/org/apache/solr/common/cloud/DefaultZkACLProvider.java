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
package org.apache.solr.common.cloud;

import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

public class DefaultZkACLProvider implements ZkACLProvider {

  private List<ACL> globalACLsToAdd;
  
  @Override
  public List<ACL> getACLsToAdd(String zNodePath) {
    // In default (simple) implementation use the same set of ACLs for all znodes
    if (globalACLsToAdd == null) {
      synchronized (this) {
        if (globalACLsToAdd == null) globalACLsToAdd = createGlobalACLsToAdd();
      }
    }
    return globalACLsToAdd;

  }
  
  protected List<ACL> createGlobalACLsToAdd() {
    return ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }

}
