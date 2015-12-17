package org.apache.solr.security;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singleton;
import static org.apache.solr.handler.admin.SecurityConfHandler.getListValue;
import static org.apache.solr.handler.admin.SecurityConfHandler.getMapValue;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.getDeepCopy;


public class RuleBasedAuthorizationPlugin implements AuthorizationPlugin, ConfigEditablePlugin {
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
              result = result == null ? new ArrayList<Permission>() : new ArrayList<Permission>(result);
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
      MatchStatus flag = checkCollPerm(mapping.get(""), context);
      return flag.rsp;
    }

    for (AuthorizationContext.CollectionRequest collreq : collectionRequests) {
      //check permissions for each collection
      MatchStatus flag = checkCollPerm(mapping.get(collreq.collectionName), context);
      if (flag != MatchStatus.NO_PERMISSIONS_FOUND) return flag.rsp;
    }
    //check global permissions.
    MatchStatus flag = checkCollPerm(mapping.get(null), context);
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
    loopPermissions:
    for (int i = 0; i < permissions.size(); i++) {
      Permission permission = permissions.get(i);
      if (permission.method != null && !permission.method.contains(context.getHttpMethod())) {
        //this permissions HTTP method does not match this rule. try other rules
        continue;
      }
      if(permission.predicate != null){
        if(!permission.predicate.test(context)) continue ;
      }

      if (permission.params != null) {
        for (Map.Entry<String, Object> e : permission.params.entrySet()) {
          String paramVal = context.getParams().get(e.getKey());
          Object val = e.getValue();
          if (val instanceof List) {
            if (!((List) val).contains(paramVal)) continue loopPermissions;
          } else if (!Objects.equals(val, paramVal)) continue loopPermissions;
        }
      }

      if (permission.role == null) {
        //no role is assigned permission.That means everybody is allowed to access
        return MatchStatus.PERMITTED;
      }
      if (principal == null) {
        log.info("request has come without principal. failed permission {} ",permission);
        //this resource needs a principal but the request has come without
        //any credential.
        return MatchStatus.USER_REQUIRED;
      } else if (permission.role.contains("*")) {
        return MatchStatus.PERMITTED;
      }

      for (String role : permission.role) {
        Set<String> userRoles = usersVsRoles.get(principal.getName());
        if (userRoles != null && userRoles.contains(role)) return MatchStatus.PERMITTED;
      }
      log.info("This resource is configured to have a permission {}, The principal {} does not have the right role ", permission, principal);
      return MatchStatus.FORBIDDEN;
    }
    log.debug("No permissions configured for the resource {} . So allowed to access", context.getResource());
    return MatchStatus.NO_PERMISSIONS_FOUND;
  }

  @Override
  public void init(Map<String, Object> initInfo) {
    mapping.put(null, new WildCardSupportMap());
    Map<String, Object> map = getMapValue(initInfo, "user-role");
    for (Object o : map.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      String roleName = (String) e.getKey();
      usersVsRoles.put(roleName, readValueAsSet(map, roleName));
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

  /**
   * read a key value as a set. if the value is a single string ,
   * return a singleton set
   *
   * @param m   the map from which to lookup
   * @param key the key with which to do lookup
   */
  static Set<String> readValueAsSet(Map m, String key) {
    Set<String> result = new HashSet<>();
    Object val = m.get(key);
    if (val == null) {
      if("collection".equals(key)){
        //for collection collection: null means a core admin/ collection admin request
        // otherwise it means a request where collection name is ignored
        return m.containsKey(key) ?  Collections.singleton("") : Collections.<String>singleton(null);
      }
      return null;
    }
    if (val instanceof Collection) {
      Collection list = (Collection) val;
      for (Object o : list) result.add(String.valueOf(o));
    } else if (val instanceof String) {
      result.add((String) val);
    } else {
      throw new RuntimeException("Bad value for : " + key);
    }
    return result.isEmpty() ? null : Collections.unmodifiableSet(result);
  }

  @Override
  public void close() throws IOException { }

  static class Permission {
    String name;
    Set<String> path, role, collections, method;
    Map<String, Object> params;
    Predicate<AuthorizationContext> predicate;

    private Permission() {
    }

    static Permission load(Map m) {
      Permission p = new Permission();
      String name = (String) m.get(NAME);
      if (!m.containsKey("role")) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "role not specified");
      p.role = readValueAsSet(m, "role");
      if (well_known_permissions.containsKey(name)) {
        HashSet<String> disAllowed = new HashSet<>(knownKeys);
        disAllowed.remove("role");//these are the only
        disAllowed.remove(NAME);//allowed keys for well-known permissions
        for (String s : disAllowed) {
          if (m.containsKey(s))
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, s + " is not a valid key for the permission : " + name);
        }
        p.predicate = (Predicate<AuthorizationContext>) ((Map) well_known_permissions.get(name)).get(Predicate.class.getName());
        m = well_known_permissions.get(name);
      }
      p.name = name;
      p.path = readSetSmart(name, m, "path");
      p.collections = readSetSmart(name, m, "collection");
      p.method = readSetSmart(name, m, "method");
      p.params = (Map<String, Object>) m.get("params");
      return p;
    }

    static final Set<String> knownKeys = ImmutableSet.of("collection", "role", "params", "path", "method", NAME);
  }

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

  /**
   * This checks for the defaults available other rules for the keys
   */
  private static Set<String> readSetSmart(String permissionName, Map m, String key) {
    Set<String> set = readValueAsSet(m, key);
    if (set == null && well_known_permissions.containsKey(permissionName)) {
      set = readValueAsSet((Map) well_known_permissions.get(permissionName), key);
    }
    if ("method".equals(key)) {
      if (set != null) {
        for (String s : set) if (!HTTP_METHODS.contains(s)) return null;
      }
      return set;
    }
    return set == null ? singleton((String)null) : set;
  }

  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    for (CommandOperation op : commands) {
      OPERATION operation = null;
      for (OPERATION o : OPERATION.values()) {
        if (o.name.equals(op.name)) {
          operation = o;
          break;
        }
      }
      if (operation == null) {
        op.unknownOperation();
        return null;
      }
      latestConf = operation.edit(latestConf, op);
      if (latestConf == null) return null;

    }
    return latestConf;
  }

  enum OPERATION {
    SET_USER_ROLE("set-user-role") {
      @Override
      public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
        Map<String, Object> roleMap = getMapValue(latestConf, "user-role");
        Map<String, Object> map = op.getDataMap();
        if (op.hasError()) return null;
        for (Map.Entry<String, Object> e : map.entrySet()) {
          if (e.getValue() == null) {
            roleMap.remove(e.getKey());
            continue;
          }
          if (e.getValue() instanceof String || e.getValue() instanceof List) {
            roleMap.put(e.getKey(), e.getValue());
          } else {
            op.addError("Unexpected value ");
            return null;
          }
        }
        return latestConf;
      }
    },
    SET_PERMISSION("set-permission") {
      @Override
      public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
        String name = op.getStr(NAME);
        Map<String, Object> dataMap = op.getDataMap();
        if (op.hasError()) return null;
        dataMap = getDeepCopy(dataMap, 3);
        String before = (String) dataMap.remove("before");
        for (String key : dataMap.keySet()) {
          if (!Permission.knownKeys.contains(key)) op.addError("Unknown key, " + key);
        }
        try {
          Permission.load(dataMap);
        } catch (Exception e) {
          op.addError(e.getMessage());
          return null;
        }
        List<Map> permissions = getListValue(latestConf, "permissions");
        List<Map> permissionsCopy = new ArrayList<>();
        boolean added = false;
        for (Map e : permissions) {
          Object n = e.get(NAME);
          if (n.equals(before) || n.equals(name)) {
            added = true;
            permissionsCopy.add(dataMap);
          }
          if (!n.equals(name)) permissionsCopy.add(e);
        }
        if (!added && before != null) {
          op.addError("Invalid 'before' :" + before);
          return null;
        }
        if (!added) permissionsCopy.add(dataMap);
        latestConf.put("permissions", permissionsCopy);
        return latestConf;
      }
    },
    UPDATE_PERMISSION("update-permission") {
      @Override
      public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
        String name = op.getStr(NAME);
        if (op.hasError()) return null;
        for (Map permission : (List<Map>) getListValue(latestConf, "permissions")) {
          if (name.equals(permission.get(NAME))) {
            LinkedHashMap copy = new LinkedHashMap<>(permission);
            copy.putAll(op.getDataMap());
            op.setCommandData(copy);
            return SET_PERMISSION.edit(latestConf, op);
          }
        }
        op.addError("No such permission " + name);
        return null;
      }
    },
    DELETE_PERMISSION("delete-permission") {
      @Override
      public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
        List<String> names = op.getStrs("");
        if (names == null || names.isEmpty()) {
          op.addError("Invalid command");
          return null;
        }
        names = new ArrayList<>(names);
        List<Map> copy = new ArrayList<>();
        List<Map> p = getListValue(latestConf, "permissions");
        for (Map map : p) {
          Object n = map.get(NAME);
          if (names.contains(n)) {
            names.remove(n);
            continue;
          } else {
            copy.add(map);
          }
        }
        if (!names.isEmpty()) {
          op.addError("Unknown permission name(s) " + names);
          return null;
        }
        latestConf.put("permissions", copy);
        return latestConf;
      }
    };

    public abstract Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op);

    public final String name;

    OPERATION(String s) {
      this.name = s;
    }

    public static OPERATION get(String name) {
      for (OPERATION o : values()) if (o.name.equals(name)) return o;
      return null;
    }
  }

  public static final Set<String> HTTP_METHODS = ImmutableSet.of("GET", "POST", "DELETE", "PUT", "HEAD");

  private static final Map<String, Map<String,Object>> well_known_permissions = (Map) Utils.fromJSONString(
          "    { " +
          "    security-edit :{" +
          "      path:['/admin/authentication','/admin/authorization']," +
          "      collection:null," +
          "      method:POST }," +
          "    security-read :{" +
          "      path:['/admin/authentication','/admin/authorization']," +
          "      collection:null," +
          "      method:GET}," +
          "    schema-edit :{" +
          "      method:POST," +
          "      path:'/schema/*'}," +
          "    collection-admin-edit :{" +
              "  collection:null," +
          "      path:'/admin/collections'}," +
          "    collection-admin-read :{" +
          "      collection:null," +
          "      path:'/admin/collections'}," +
          "    schema-read :{" +
          "      method:GET," +
          "      path:'/schema/*'}," +
          "    config-read :{" +
          "      method:GET," +
          "      path:'/config/*'}," +
          "    update :{" +
          "      path:'/update/*'}," +
          "    read :{" +
          "      path:['/select', '/get','/browse','/tvrh','/terms','/clustering','/elevate', '/export','/spell','/clustering']}," +
          "    config-edit:{" +
          "      method:POST," +
          "      path:'/config/*'}}");

  static {
    ((Map) well_known_permissions.get("collection-admin-edit")).put(Predicate.class.getName(), getCollectionActionPredicate(true));
    ((Map) well_known_permissions.get("collection-admin-read")).put(Predicate.class.getName(), getCollectionActionPredicate(false));
  }

  private static Predicate<AuthorizationContext> getCollectionActionPredicate(final boolean isEdit) {
    return new Predicate<AuthorizationContext>() {
      @Override
      public boolean test(AuthorizationContext context) {
        String action = context.getParams().get("action");
        if (action == null) return false;
        CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(action);
        if (collectionAction == null) return false;
        return isEdit ? collectionAction.isWrite : !collectionAction.isWrite;
      }
    };
  }


  public static void main(String[] args) {
    System.out.println(Utils.toJSONString(well_known_permissions));

  }
  /**For making the code work in Java 7.
   * In trunk it uses the Predicate interface in jdk
   * //todo remove when upgrading to java 8
   */
  public interface Predicate<T> {

    boolean test(T t);
  }

}
