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
package org.apache.solr.request;

import java.util.function.Predicate;

import org.apache.lucene.util.BytesRef;
import org.apache.commons.lang3.StringUtils;

/**
 * An implementation of {@link Predicate} which returns true if the BytesRef contains a given substring.
 */
public class SubstringBytesRefFilter implements Predicate<BytesRef> {
  final private String contains;
  final private boolean ignoreCase;
  
  public SubstringBytesRefFilter(String contains, boolean ignoreCase) {
    this.contains = contains;
    this.ignoreCase = ignoreCase;
  }

  public String substring() {
    return contains;
  }

  protected boolean includeString(String term) {
    if (ignoreCase) {
      return StringUtils.containsIgnoreCase(term, contains);
    }

    return StringUtils.contains(term, contains);
  }

  @Override
  public boolean test(BytesRef term) {
    return includeString(term.utf8ToString());
  }
}
