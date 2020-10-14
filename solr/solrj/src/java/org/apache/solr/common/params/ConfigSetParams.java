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
package org.apache.solr.common.params;

import java.util.Locale;

/**
 * ConfigSets API related parameters and actions.
 */
public interface ConfigSetParams
{
  public final static String ACTION = "action";
  public final static String OVERWRITE = "overwrite";
  public final static String CLEANUP = "cleanup";
  public final static String FILE_PATH = "filePath";

  public enum ConfigSetAction {
    CREATE,
    UPLOAD,
    DELETE,
    LIST;

    public static ConfigSetAction get(String p) {
      if (p != null) {
        try {
          return ConfigSetAction.valueOf( p.toUpperCase(Locale.ROOT) );
        } catch (Exception ex) {}
      }
      return null;
    }

    public boolean isEqual(String s) {
      if (s == null) return false;
      return toString().equals(s.toUpperCase(Locale.ROOT));
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }
  }
}
