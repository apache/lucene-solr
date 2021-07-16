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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.solr.common.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import static org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider.readCredentialsFile;

public class VMParamsAllAndReadonlyDigestZkACLProvider extends SecurityAwareZkACLProvider {

  public static final String DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME = "zkDigestReadonlyUsername";
  public static final String DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME = "zkDigestReadonlyPassword";
  
  final String zkDigestAllUsernameVMParamName;
  final String zkDigestAllPasswordVMParamName;
  final String zkDigestReadonlyUsernameVMParamName;
  final String zkDigestReadonlyPasswordVMParamName;
  final Properties credentialsProps;
  
  public VMParamsAllAndReadonlyDigestZkACLProvider() {
    this(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, 
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME
        );
  }
  
  public VMParamsAllAndReadonlyDigestZkACLProvider(String zkDigestAllUsernameVMParamName, String zkDigestAllPasswordVMParamName,
      String zkDigestReadonlyUsernameVMParamName, String zkDigestReadonlyPasswordVMParamName) {
    this.zkDigestAllUsernameVMParamName = zkDigestAllUsernameVMParamName;
    this.zkDigestAllPasswordVMParamName = zkDigestAllPasswordVMParamName;
    this.zkDigestReadonlyUsernameVMParamName = zkDigestReadonlyUsernameVMParamName;
    this.zkDigestReadonlyPasswordVMParamName = zkDigestReadonlyPasswordVMParamName;

    String pathToFile = System.getProperty(DEFAULT_DIGEST_FILE_VM_PARAM_NAME);
    credentialsProps = (pathToFile != null) ? readCredentialsFile(pathToFile) : System.getProperties();
  }

  /**
   * @return Set of ACLs to return for non-security related znodes
   */
  @Override
  protected List<ACL> createNonSecurityACLsToAdd() {
    return createACLsToAdd(true);
  }

  /**
   * @return Set of ACLs to return security-related znodes
   */
  @Override
  protected List<ACL> createSecurityACLsToAdd() {
    return createACLsToAdd(false);
  }

  protected List<ACL> createACLsToAdd(boolean includeReadOnly) {
    String digestAllUsername = credentialsProps.getProperty(zkDigestAllUsernameVMParamName);
    String digestAllPassword = credentialsProps.getProperty(zkDigestAllPasswordVMParamName);
    String digestReadonlyUsername = credentialsProps.getProperty(zkDigestReadonlyUsernameVMParamName);
    String digestReadonlyPassword = credentialsProps.getProperty(zkDigestReadonlyPasswordVMParamName);

    return createACLsToAdd(includeReadOnly,
        digestAllUsername, digestAllPassword,
        digestReadonlyUsername, digestReadonlyPassword);
  }

  /**
   * Note: only used for tests
   */
  protected List<ACL> createACLsToAdd(boolean includeReadOnly,
                                      String digestAllUsername, String digestAllPassword,
                                      String digestReadonlyUsername, String digestReadonlyPassword) {

      try {
      List<ACL> result = new ArrayList<ACL>();
  
      // Not to have to provide too much credentials and ACL information to the process it is assumed that you want "ALL"-acls
      // added to the user you are using to connect to ZK (if you are using VMParamsSingleSetCredentialsDigestZkCredentialsProvider)
      if (!StringUtils.isEmpty(digestAllUsername) && !StringUtils.isEmpty(digestAllPassword)) {
        result.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(digestAllUsername + ":" + digestAllPassword))));
      }

      if (includeReadOnly) {
        // Besides that support for adding additional "READONLY"-acls for another user
        if (!StringUtils.isEmpty(digestReadonlyUsername) && !StringUtils.isEmpty(digestReadonlyPassword)) {
          result.add(new ACL(ZooDefs.Perms.READ, new Id("digest", DigestAuthenticationProvider.generateDigest(digestReadonlyUsername + ":" + digestReadonlyPassword))));
        }
      }
      
      if (result.isEmpty()) {
        result = ZooDefs.Ids.OPEN_ACL_UNSAFE;
      }
      
      return result;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

}

