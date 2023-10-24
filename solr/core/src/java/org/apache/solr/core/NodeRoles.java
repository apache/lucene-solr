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
package org.apache.solr.core;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;

public class NodeRoles {
  public static final String NODE_ROLES_PROP = "solr.node.roles";

  /** Roles to be assumed on nodes that don't have roles specified for them at startup */
  public static final String DEFAULT_ROLES_STRING = "data:on,overseer:allowed";

  // Map of roles to mode that are applicable for this node.
  private Map<Role, String> nodeRoles;

  public NodeRoles(String rolesString) {
    Map<Role, String> roles = new EnumMap<>(Role.class);
    if (StringUtils.isEmpty(rolesString)) {
      rolesString = DEFAULT_ROLES_STRING;
    }
    List<String> rolesList = StrUtils.splitSmart(rolesString, ',');
    for (String s : rolesList) {
      List<String> roleMode = StrUtils.splitSmart(s, ':');
      Role r = Role.getRole(roleMode.get(0));
      String m = roleMode.get(1);
      if (r.supportedModes().contains(m)) {
        roles.put(r, m);
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unknown role mode '" + roleMode.get(1) + "' for role '" + r + "'");
      }
    }
    for (Role r : Role.values()) {
      if (!roles.containsKey(r)) {
        roles.put(r, r.modeWhenRoleIsAbsent());
      }
    }
    nodeRoles = Collections.unmodifiableMap(roles);
  }

  public Map<Role, String> getRoles() {
    return nodeRoles;
  }

  public String getRoleMode(Role role) {
    return nodeRoles.get(role);
  }

  public boolean isOverseerAllowedOrPreferred() {
    String roleMode = nodeRoles.get(Role.OVERSEER);
    return MODE_ALLOWED.equals(roleMode) || MODE_PREFERRED.equals(roleMode);
  }

  public static final String MODE_ON = "on";
  public static final String MODE_OFF = "off";
  public static final String MODE_ALLOWED = "allowed";
  public static final String MODE_PREFERRED = "preferred";
  public static final String MODE_DISALLOWED = "disallowed";

  public enum Role {
    DATA("data") {
      @Override
      public Set<String> supportedModes() {
        return ImmutableSet.of(MODE_ON, MODE_OFF);
      }

      @Override
      public String modeWhenRoleIsAbsent() {
        return MODE_OFF;
      }
    },
    OVERSEER("overseer") {
      @Override
      public Set<String> supportedModes() {
        return ImmutableSet.of(MODE_ALLOWED, MODE_PREFERRED, MODE_DISALLOWED);
      }

      @Override
      public String modeWhenRoleIsAbsent() {
        return MODE_DISALLOWED;
      }
    },

    COORDINATOR("coordinator") {
      @Override
      public String modeWhenRoleIsAbsent() {
        return MODE_OFF;
      }

      @Override
      public Set<String> supportedModes() {
        return ImmutableSet.of(MODE_ON, MODE_OFF);
      }
    };

    public final String roleName;

    Role(String name) {
      this.roleName = name;
    }

    public static Role getRole(String value) {
      for (Role role : Role.values()) {
        if (value.equals(role.roleName)) return role;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown role: " + value);
    }

    public abstract Set<String> supportedModes();

    /** Default mode for a role in nodes where this role is not specified. */
    public abstract String modeWhenRoleIsAbsent();

    @Override
    public String toString() {
      return roleName;
    }
  }

  public static String getZNodeForRole(Role role) {
    return ZkStateReader.NODE_ROLES + "/" + role.roleName;
  }

  public static String getZNodeForRoleMode(Role role, String mode) {
    return ZkStateReader.NODE_ROLES + "/" + role.roleName + "/" + mode;
  }
}
