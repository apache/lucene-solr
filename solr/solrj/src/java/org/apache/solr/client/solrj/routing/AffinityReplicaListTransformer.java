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
package org.apache.solr.client.solrj.routing;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;

/**
 * Allows better caching by establishing deterministic evenly-distributed replica routing preferences according to
 * either explicitly configured hash routing parameter, or the hash of a query parameter (configurable, usually related
 * to the main query).
 */
public class AffinityReplicaListTransformer implements ReplicaListTransformer {

  private final int routingDividend;

  private AffinityReplicaListTransformer(String hashVal) {
    this.routingDividend = Math.abs(Hash.lookup3ycs(hashVal, 0, hashVal.length(), 0));
  }

  private AffinityReplicaListTransformer(int routingDividend) {
    this.routingDividend = routingDividend;
  }

  /**
   *
   * @param dividendParam int param to be used directly for mod-based routing
   * @param hashParam String param to be hashed into an int for mod-based routing
   * @param requestParams the parameters of the Solr request
   * @return null if specified routing vals are not able to be parsed properly
   */
  public static ReplicaListTransformer getInstance(String dividendParam, String hashParam, SolrParams requestParams) {
    Integer dividendVal;
    if (dividendParam != null && (dividendVal = requestParams.getInt(dividendParam)) != null) {
      return new AffinityReplicaListTransformer(dividendVal);
    }
    String hashVal;
    if (hashParam != null && (hashVal = requestParams.get(hashParam)) != null && !hashVal.isEmpty()) {
      return new AffinityReplicaListTransformer(hashVal);
    } else {
      return null;
    }
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void transform(List<?> choices) {
    int size = choices.size();
    if (size > 1) {
      int i = 0;
      SortableChoice[] sortableChoices = new SortableChoice[size];
      for (Object o : choices) {
        sortableChoices[i++] = new SortableChoice(o);
      }
      Arrays.sort(sortableChoices, SORTABLE_CHOICE_COMPARATOR);
      ListIterator iter = choices.listIterator();
      i = routingDividend % size;
      final int limit = i + size;
      do {
        iter.next();
        iter.set(sortableChoices[i % size].choice);
      } while (++i < limit);
    }
  }

  private static final class SortableChoice {

    private final Object choice;
    private final String sortableCoreLabel;

    private SortableChoice(Object choice) {
      this.choice = choice;
      if (choice instanceof Replica) {
        this.sortableCoreLabel = ((Replica)choice).getCoreUrl();
      } else if (choice instanceof String) {
        this.sortableCoreLabel = (String)choice;
      } else {
        throw new IllegalArgumentException("can't handle type " + choice.getClass());
      }
    }

  }

  private static final Comparator<SortableChoice> SORTABLE_CHOICE_COMPARATOR = Comparator.comparing(o -> o.sortableCoreLabel);
}
