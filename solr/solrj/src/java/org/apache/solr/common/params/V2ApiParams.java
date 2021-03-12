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

/**
 * String constants used in sending and receiving V2 API requests.
 */
public class V2ApiParams {
  private V2ApiParams() { /* Private ctor prevents instantiation */ }

  public static final String COLLECTIONS_API_PATH = "/collections";
  public static final String C_API_PATH = "/c";

  public static final String BACKUPS_API_PATH = COLLECTIONS_API_PATH + "/backups";
  public static final String BACKUPS_API_SHORT_PATH = C_API_PATH + "/backups";

  public static final String LIST_BACKUPS_CMD = "list-backups";
  public static final String DELETE_BACKUPS_CMD = "delete-backups";
}
