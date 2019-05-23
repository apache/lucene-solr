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
package org.apache.solr.security;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.common.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.solr.handler.admin.SecurityConfHandler.getListValue;
import static org.apache.solr.handler.admin.SecurityConfHandler.getMapValue;


public class RuleBasedAuthorizationPlugin implements AuthorizationPlugin, ConfigEditablePlugin, SpecProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, Set<String>> usersVsRoles = new HashMap<>();
  private final Map<String, WildCardSupportMap> mapping = new HashMap<>();
  private final List<Permission> permissions = new ArrayList<>();


  private static class WildCardSupportMap extends HashMap<String, List<Permission>> {
    final Set<String> wildcardPrefixes = new HashSet<>();

    @Override
    public List<Permission> put(String key, List<Permission> value) {
      if (key != null && key.endsWith("/*")) {
        key = key.substring(0, key.length() - 2);
        wildcardPrefixes.add(key);
      }
      return super.put(key, value);
    }

    @Override
    public List<Permission> get(Object key) {
      List<Permission> result = super.get(key);
      if (key == null || result != null) return result;
      if (!wildcardPrefixes.isEmpty()) {
        for (String s : wildcardPrefixes) {
          if (key.toString().startsWith(s)) {
            List<Permission> l = super.get(s);
            if (l != null) {
              result = result == null ? new ArrayList<>() : new ArrayList<>(result);
              result.addAll(l);
            }
          }
        }
      }
      return result;
    }
  }

  @Override
  public AuthorizationResponse authorize(AuthorizationContext context) {
    List<AuthorizationContext.CollectionRequest> collectionRequests = context.getCollectionRequests();
    if (context.getRequestType() == AuthorizationContext.RequestType.ADMIN) {
      MatchStatus flag = checkCollPerm(mapping.get(null), context);
      return flag.rsp;
    }

    for (AuthorizationContext.CollectionRequest collreq : collectionRequests) {
      //check permissions for each collection
      MatchStatus flag = checkCollPerm(mapping.get(collreq.collectionName), context);
      if (flag != MatchStatus.NO_PERMISSIONS_FOUND) return flag.rsp;
    }
    //check wildcard (all=*) permissions.
    MatchStatus flag = checkCollPerm(mapping.get("*"), context);
    return flag.rsp;
  }

  private MatchStatus checkCollPerm(Map<String, List<Permission>> pathVsPerms,
                                    AuthorizationContext context) {
    if (pathVsPerms == null) return MatchStatus.NO_PERMISSIONS_FOUND;

    String path = context.getResource();
    MatchStatus flag = checkPathPerm(pathVsPerms.get(path), context);
    if (flag != MatchStatus.NO_PERMISSIONS_FOUND) return flag;
    return checkPathPerm(pathVsPerms.get(null), context);
  }

  private MatchStatus checkPathPerm(List<Permission> permissions, AuthorizationContext context) {
    if (permissions == null || permissions.isEmpty()) return MatchStatus.NO_PERMISSIONS_FOUND;
    Principal principal = context.getUserPrincipal();

    final Permission governingPermission = findFirstGoverningPermission(permissions, context);
    if (governingPermission == null) {
      log.debug("No permissions configured for the resource {} . So allowed to access", context.getResource());
      return MatchStatus.NO_PERMISSIONS_FOUND;
    }

    return determineIfPermissionPermitsPrincipal(principal, governingPermission);
  }

  private Permission findFirstGoverningPermission(List<Permission> permissions, AuthorizationContext context) {
    for (int i = 0; i < permissions.size(); i++) {
      Permission permission = permissions.get(i);
      if (permissionAppliesToRequest(permission, context)) return permission;
    }

    return null;
  }

  private boolean permissionAppliesToRequest(Permission permission, AuthorizationContext context) {
    if (PermissionNameProvider.values.containsKey(permission.name)) {
      return predefinedPermissionAppliesToRequest(permission, context);
    } else {
      return customPermissionAppliesToRequest(permission, context);
    }
  }

  private boolean predefinedPermissionAppliesToRequest(Permission predefinedPermission, AuthorizationContext context) {
    if (predefinedPermission.wellknownName == PermissionNameProvider.Name.ALL) {
      return true; //'ALL' applies to everything!
    } else if (! (context.getHandler() instanceof PermissionNameProvider)) {
      return false; // We're not 'ALL', and the handler isn't associated with any other predefined permissions
    } else {
      PermissionNameProvider handler = (PermissionNameProvider) context.getHandler();
      PermissionNameProvider.Name permissionName = handler.getPermissionName(context);

      return permissionName != null && predefinedPermission.name.equals(permissionName.name);
    }
  }

  private boolean customPermissionAppliesToRequest(Permission customPermission, AuthorizationContext context) {
    if (customPermission.method != null && !customPermission.method.contains(context.getHttpMethod())) {
      //this permissions HTTP method does not match this rule. try other rules
      return false;
    }
    if (customPermission.params != null) {
      for (Map.Entry<String, Function<String[], Boolean>> e : customPermission.params.entrySet()) {
        String[] paramVal = context.getParams().getParams(e.getKey());
        if(!e.getValue().apply(paramVal)) return false;
      }
    }

    return true;
  }

  private MatchStatus determineIfPermissionPermitsPrincipal(Principal principal, Permission governingPermission) {
    if (governingPermission.role == null) {
      //no role is assigned permission.That means everybody is allowed to access
      return MatchStatus.PERMITTED;
    }
    if (principal == null) {
      log.info("request has come without principal. failed permission {} ", governingPermission);
      //this resource needs a principal but the request has come without
      //any credential.
      return MatchStatus.USER_REQUIRED;
    } else if (governingPermission.role.contains("*")) {
      return MatchStatus.PERMITTED;
    }

    for (String role : governingPermission.role) {
      Set<String> userRoles = usersVsRoles.get(principal.getName());
      if (userRoles != null && userRoles.contains(role)) return MatchStatus.PERMITTED;
    }
    log.info("This resource is configured to have a permission {}, The principal {} does not have the right role ", governingPermission, principal);
    return MatchStatus.FORBIDDEN;
  }

  @Override
  public void init(Map<String, Object> initInfo) {
    mapping.put(null, new WildCardSupportMap());
    Map<String, Object> map = getMapValue(initInfo, "user-role");
    for (Object o : map.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      String roleName = (String) e.getKey();
      usersVsRoles.put(roleName, Permission.readValueAsSet(map, roleName));
    }
    List<Map> perms = getListValue(initInfo, "permissions");
    for (Map o : perms) {
      Permission p;
      try {
        p = Permission.load(o);
      } catch (Exception exp) {
        log.error("Invalid permission ", exp);
        continue;
      }
      permissions.add(p);
      add2Mapping(p);
    }
  }

  //this is to do optimized lookup of permissions for a given collection/path
  private void add2Mapping(Permission permission) {
    for (String c : permission.collections) {
      WildCardSupportMap m = mapping.get(c);
      if (m == null) mapping.put(c, m = new WildCardSupportMap());
      for (String path : permission.path) {
        List<Permission> perms = m.get(path);
        if (perms == null) m.put(path, perms = new ArrayList<>());
        perms.add(permission);
      }
    }
  }


  @Override
  public void close() throws IOException { }

  enum MatchStatus {
    USER_REQUIRED(AuthorizationResponse.PROMPT),
    NO_PERMISSIONS_FOUND(AuthorizationResponse.OK),
    PERMITTED(AuthorizationResponse.OK),
    FORBIDDEN(AuthorizationResponse.FORBIDDEN);

    final AuthorizationResponse rsp;

    MatchStatus(AuthorizationResponse rsp) {
      this.rsp = rsp;
    }
  }



  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    for (CommandOperation op : commands) {
      AutorizationEditOperation operation = ops.get(op.name);
      if (operation == null) {
        op.unknownOperation();
        return null;
      }
      latestConf = operation.edit(latestConf, op);
      if (latestConf == null) return null;

    }
    return latestConf;
  }

  private static final Map<String, AutorizationEditOperation> ops = unmodifiableMap(asList(AutorizationEditOperation.values()).stream().collect(toMap(AutorizationEditOperation::getOperationName, identity())));


  @Override
  public ValidatingJsonMap getSpec() {
    return Utils.getSpec("cluster.security.RuleBasedAuthorization").getSpec();

  }
}
