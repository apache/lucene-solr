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

import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.CommandOperation;

/**An interface to be implemented by a Plugin whose Configuration is runtime editable
 *
 */
public interface ConfigEditablePlugin {


  /** Operate the commands on the latest conf and return a new conf object
   * If there are errors in the commands , throw a SolrException. return a null
   * if no changes are to be made as a result of this edit. It is the responsibility
   * of the implementation to ensure that the returned config is valid . The framework
   * does no validation of the data
   */
  Map<String,Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands);

}
