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

import java.util.Set;

/**
 * Interface used to pass verified user roles in a Principal object.
 * An Authorization plugin may check for the presence of verified user
 * roles on the Principal and choose to use those roles instead of
 * explicitly configuring roles in config. Such roles may e.g. origin
 * from a signed and validated JWT token.
 */
public interface VerifiedUserRoles {
  /**
   * Gets a set of roles that have been verified to belong to a user
   */
  Set<String> getVerifiedRoles();
}
