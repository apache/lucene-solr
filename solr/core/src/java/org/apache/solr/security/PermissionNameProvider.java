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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * A requestHandler should implement this interface to provide the well known permission
 * at request time
 */
public interface PermissionNameProvider {
  enum Name {
    COLL_EDIT_PERM("collection-admin-edit", null),
    COLL_READ_PERM("collection-admin-read", null),
    CORE_READ_PERM("core-admin-read", null),
    CORE_EDIT_PERM("core-admin-edit", null),
    ZK_READ_PERM("zk-read", null),
    READ_PERM("read", "*"),
    UPDATE_PERM("update", "*"),
    CONFIG_EDIT_PERM("config-edit", unmodifiableSet(new HashSet<>(asList("*", null)))),
    CONFIG_READ_PERM("config-read", unmodifiableSet(new HashSet<>(asList("*", null)))),
    SCHEMA_READ_PERM("schema-read", "*"),
    SCHEMA_EDIT_PERM("schema-edit", "*"),
    SECURITY_EDIT_PERM("security-edit", null),
    SECURITY_READ_PERM("security-read", null),
    METRICS_READ_PERM("metrics-read", null),
    AUTOSCALING_READ_PERM("autoscaling-read", null),
    AUTOSCALING_WRITE_PERM("autoscaling-write", null),
    AUTOSCALING_HISTORY_READ_PERM("autoscaling-history-read", null),
    METRICS_HISTORY_READ_PERM("metrics-history-read", null),
    FILESTORE_READ_PERM("filestore-read", null),
    FILESTORE_WRITE_PERM("filestore-write", null),
    PACKAGE_EDIT_PERM("package-edit", null),
    PACKAGE_READ_PERM("package-read", null),

    ALL("all", unmodifiableSet(new HashSet<>(asList("*", null))))
    ;
    final String name;
    final Set<String> collName;

    @SuppressWarnings({"unchecked"})
    Name(String s, Object collName) {
      name = s;
      this.collName = collName instanceof Set? (Set<String>)collName : singleton((String)collName);
    }

    public static Name get(String s) {
      return values.get(s);
    }

    public String getPermissionName() {
      return name;
    }
  }

  Set<String> NULL = singleton(null);
  Set<String> ANY = singleton("*");

  Map<String, Name> values = unmodifiableMap(asList(Name.values()).stream().collect(toMap(Name::getPermissionName, identity())));

  Name getPermissionName(AuthorizationContext request);
}
