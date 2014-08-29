package org.apache.solr.common.cloud;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

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

public class VMParamsAllAndReadonlyDigestZkACLProvider extends DefaultZkACLProvider {

  public static final String DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME = "zkDigestReadonlyUsername";
  public static final String DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME = "zkDigestReadonlyPassword";
  
  final String zkDigestAllUsernameVMParamName;
  final String zkDigestAllPasswordVMParamName;
  final String zkDigestReadonlyUsernameVMParamName;
  final String zkDigestReadonlyPasswordVMParamName;
  
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
  }


  @Override
  protected List<ACL> createGlobalACLsToAdd() {
    try {
      List<ACL> result = new ArrayList<ACL>();
  
      // Not to have to provide too much credentials and ACL information to the process it is assumed that you want "ALL"-acls
      // added to the user you are using to connect to ZK (if you are using VMParamsSingleSetCredentialsDigestZkCredentialsProvider)
      String digestAllUsername = System.getProperty(zkDigestAllUsernameVMParamName);
      String digestAllPassword = System.getProperty(zkDigestAllPasswordVMParamName);
      if (!StringUtils.isEmpty(digestAllUsername) && !StringUtils.isEmpty(digestAllPassword)) {
        result.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(digestAllUsername + ":" + digestAllPassword))));
      }
  
      // Besides that support for adding additional "READONLY"-acls for another user
      String digestReadonlyUsername = System.getProperty(zkDigestReadonlyUsernameVMParamName);
      String digestReadonlyPassword = System.getProperty(zkDigestReadonlyPasswordVMParamName);
      if (!StringUtils.isEmpty(digestReadonlyUsername) && !StringUtils.isEmpty(digestReadonlyPassword)) {
        result.add(new ACL(ZooDefs.Perms.READ, new Id("digest", DigestAuthenticationProvider.generateDigest(digestReadonlyUsername + ":" + digestReadonlyPassword))));
      }
      
      if (result.isEmpty()) {
        result = super.createGlobalACLsToAdd();
      }
      
      return result;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

}

