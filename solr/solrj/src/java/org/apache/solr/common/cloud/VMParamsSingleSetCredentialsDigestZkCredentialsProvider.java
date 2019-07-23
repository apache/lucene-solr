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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.common.StringUtils;

public class VMParamsSingleSetCredentialsDigestZkCredentialsProvider extends DefaultZkCredentialsProvider {
  
  public static final String DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME = "zkDigestUsername";
  public static final String DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME = "zkDigestPassword";
  
  final String zkDigestUsernameVMParamName;
  final String zkDigestPasswordVMParamName;
  
  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider() {
    this(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
  }
  
  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider(String zkDigestUsernameVMParamName, String zkDigestPasswordVMParamName) {
    this.zkDigestUsernameVMParamName = zkDigestUsernameVMParamName;
    this.zkDigestPasswordVMParamName = zkDigestPasswordVMParamName;
  }

  @Override
  protected Collection<ZkCredentials> createCredentials() {
    List<ZkCredentials> result = new ArrayList<ZkCredentials>();
    String digestUsername = System.getProperty(zkDigestUsernameVMParamName);
    String digestPassword = System.getProperty(zkDigestPasswordVMParamName);
    if (!StringUtils.isEmpty(digestUsername) && !StringUtils.isEmpty(digestPassword)) {
      result.add(new ZkCredentials("digest",
          (digestUsername + ":" + digestPassword).getBytes(StandardCharsets.UTF_8)));
    }
    return result;
  }
  
}

