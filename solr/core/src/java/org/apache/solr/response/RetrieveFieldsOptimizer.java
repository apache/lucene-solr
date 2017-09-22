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

package org.apache.solr.response;

import java.util.HashSet;
import java.util.Set;

import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrReturnFields;

public class RetrieveFieldsOptimizer {
  // null means get all available stored fields
  private final Set<String> storedFields;
  // always non null
  private final Set<String> dvFields;

  RetrieveFieldsOptimizer(Set<String> storedFields, Set<String> dvFields) {
    this.storedFields = storedFields;
    this.dvFields = dvFields;
  }

  /**
   * Sometimes we could fetch a field value from either the stored document or docValues.
   * Such fields have both and are single-valued.
   * If choosing docValues allows us to avoid accessing the stored document altogether
   * for all fields to be returned then we do it,
   * otherwise we prefer the stored value when we have a choice.
   */
  void optimize(SolrDocumentFetcher docFetcher) {
    optimize(docFetcher.getAllSingleDV());
  }

  void optimize(Set<String> singleDVs) {
    if (storedFields == null) return;
    if (!singleDVs.containsAll(storedFields)) return;
    dvFields.addAll(storedFields);
    storedFields.clear();
  }

  boolean returnStoredFields() {
    return !(storedFields != null && storedFields.isEmpty());
  }

  boolean returnDVFields() {
    return !dvFields.isEmpty();
  }

  Set<String> getStoredFields() {
    return storedFields;
  }

  Set<String> getDvFields() {
    return dvFields;
  }

  public static RetrieveFieldsOptimizer create(SolrDocumentFetcher docFetcher, ReturnFields returnFields) {
    Set<String> storedFields = calcStoredFieldsForReturn(docFetcher, returnFields);
    Set<String> dvFields = calcDocValueFieldsForReturn(docFetcher, returnFields);

    return new RetrieveFieldsOptimizer(storedFields, dvFields);
  }

  private static Set<String> calcStoredFieldsForReturn(SolrDocumentFetcher docFetcher, ReturnFields returnFields) {
    final Set<String> storedFields = new HashSet<>();
    Set<String> fnames = returnFields.getLuceneFieldNames();
    if (returnFields.wantsAllFields()) {
      return null;
    } else if (returnFields.hasPatternMatching()) {
      for (String s : docFetcher.getAllStored()) {
        if (returnFields.wantsField(s)) {
          storedFields.add(s);
        }
      }
    } else if (fnames != null) {
      storedFields.addAll(fnames);
    }
    storedFields.remove(SolrReturnFields.SCORE);
    return storedFields;
  }

  private static Set<String> calcDocValueFieldsForReturn(SolrDocumentFetcher docFetcher, ReturnFields returnFields) {
    // always return not null
    final Set<String> result = new HashSet<>();
    if (returnFields.wantsAllFields()) {
      result.addAll(docFetcher.getNonStoredDVs(true));
      // check whether there are no additional fields
      Set<String> fieldNames = returnFields.getLuceneFieldNames(true);
      if (fieldNames != null) {
        // add all requested fields that may be useDocValuesAsStored=false
        for (String fl : fieldNames) {
          if (docFetcher.getNonStoredDVs(false).contains(fl)) {
            result.add(fl);
          }
        }
      }
    } else if (returnFields.hasPatternMatching()) {
      for (String s : docFetcher.getNonStoredDVs(true)) {
        if (returnFields.wantsField(s)) {
          result.add(s);
        }
      }
    } else {
      Set<String> fnames = returnFields.getLuceneFieldNames();
      if (fnames != null) {
        result.addAll(fnames);
        // here we get all non-stored dv fields because even if a user has set
        // useDocValuesAsStored=false in schema, he may have requested a field
        // explicitly using the fl parameter
        result.retainAll(docFetcher.getNonStoredDVs(false));
      }
    }
    return result;
  }
}
