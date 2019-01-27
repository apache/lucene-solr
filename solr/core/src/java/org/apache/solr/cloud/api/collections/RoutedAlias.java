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

package org.apache.solr.cloud.api.collections;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.update.AddUpdateCommand;

public interface RoutedAlias {
  String ROUTED_ALIAS_NAME_CORE_PROP = "routedAliasName"; // core prop

  static SolrException newAliasMustExistException(String aliasName) {
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        "Routed alias " + aliasName + " appears to have been removed during request processing.");
  }

  /**
   * Ensure our parsed version of the alias collection list is up to date. If it was modified, return true.
   * Note that this will return true if some other alias was modified or if properties were modified. These
   * are spurious and the caller should be written to be tolerant of no material changes.
   */
  boolean updateParsedCollectionAliases(ZkController zkController);

  String getAliasName();

  /**
   * Parses the elements of the collection list. Result is returned them in sorted order (desc) if there
   * is a natural order for this type of routed alias
   */
  List<Map.Entry<Instant,String>> parseCollections(Aliases aliases);

  /**
   * Check that the value we will be routing on is legal for this type of routed alias.
   *
   * @param cmd the command containing the document
   */
  void validateRouteValue(AddUpdateCommand cmd);

  /**
   * Create any required collections and return the name of the collection to which the current document should be sent.
   *
   * @param cmd The command that might cause collection creation
   * @return The name of the proper destination collection for the document which may or may not be a
   *         newly created collection
   */
  String createCollectionsIfRequired( AddUpdateCommand cmd);
}
