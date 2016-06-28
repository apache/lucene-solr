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
package org.apache.solr.cloud.overseer;

import java.util.Locale;

/**
 * Enum of actions supported by the overseer only.
 *
 * There are other actions supported which are public and defined
 * in {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
 */
public enum OverseerAction {
  LEADER,
  DELETECORE,
  ADDROUTINGRULE,
  REMOVEROUTINGRULE,
  UPDATESHARDSTATE,
  STATE,
  QUIT,
  DOWNNODE;

  public static OverseerAction get(String p) {
    if (p != null) {
      try {
        return OverseerAction.valueOf(p.toUpperCase(Locale.ROOT));
      } catch (Exception ex) {
      }
    }
    return null;
  }

  public boolean isEqual(String s) {
    return s != null && toString().equals(s.toUpperCase(Locale.ROOT));
  }

  public String toLower() {
    return toString().toLowerCase(Locale.ROOT);
  }
}

