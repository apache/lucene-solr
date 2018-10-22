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

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/**
 * ZkACLProvider that gives all permissions for the user specified in System
 * property "solr.authorization.superuser" (default: "solr") when using sasl,
 * and gives read permissions for anyone else.  Designed for a setup where
 * configurations have already been set up and will not be modified, or
 * where configuration changes are controlled via Solr APIs.
 */
public class SaslZkACLProvider extends SecurityAwareZkACLProvider {

  private static String superUser = System.getProperty("solr.authorization.superuser", "solr");

  @Override
  protected List<ACL> createNonSecurityACLsToAdd() {
    List<ACL> ret = new ArrayList<ACL>();
    ret.add(new ACL(ZooDefs.Perms.ALL, new Id("sasl", superUser)));
    ret.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    return ret;
  }

  @Override
  protected List<ACL> createSecurityACLsToAdd() {
    List<ACL> ret = new ArrayList<ACL>();
    ret.add(new ACL(ZooDefs.Perms.ALL, new Id("sasl", superUser)));
    return ret;
  }
}
