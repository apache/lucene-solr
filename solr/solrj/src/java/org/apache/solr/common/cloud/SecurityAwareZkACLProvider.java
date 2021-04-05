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

import org.apache.zookeeper.data.ACL;

/**
 * {@link ZkACLProvider} capable of returning a different set of
 * {@link ACL}s for security-related znodes (default: subtree under /security and security.json)
 * vs non-security-related znodes.
 */
public abstract class SecurityAwareZkACLProvider implements ZkACLProvider {
  public static final String SECURITY_ZNODE_PATH = "/security";

  private List<ACL> nonSecurityACLsToAdd;
  private List<ACL> securityACLsToAdd;


  @Override
  public final List<ACL> getACLsToAdd(String zNodePath) {
    if (isSecurityZNodePath(zNodePath)) {
      return getSecurityACLsToAdd();
    } else {
      return getNonSecurityACLsToAdd();
    }
  }

  protected boolean isSecurityZNodePath(String zNodePath) {
    return zNodePath != null
        && (zNodePath.equals(ZkStateReader.SOLR_SECURITY_CONF_PATH) || zNodePath.equals(SECURITY_ZNODE_PATH) || zNodePath.startsWith(SECURITY_ZNODE_PATH + "/"));
  }

  /**
   * @return Set of ACLs to return for non-security related znodes
   */
  protected abstract List<ACL> createNonSecurityACLsToAdd();

  /**
   * @return Set of ACLs to return security-related znodes
   */
  protected abstract List<ACL> createSecurityACLsToAdd();

  private List<ACL> getNonSecurityACLsToAdd() {
    if (nonSecurityACLsToAdd == null) {
      synchronized (this) {
        if (nonSecurityACLsToAdd == null) nonSecurityACLsToAdd = createNonSecurityACLsToAdd();
      }
    }
    return nonSecurityACLsToAdd;
  }

  private List<ACL> getSecurityACLsToAdd() {
    if (securityACLsToAdd == null) {
      synchronized (this) {
        if (securityACLsToAdd == null) securityACLsToAdd = createSecurityACLsToAdd();
      }
    }
    return securityACLsToAdd;
  }
}
