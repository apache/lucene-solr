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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.BytesRef;

/**
 * An implementation of {@link Predicate} which returns true if the BytesRef matches the supplied regular expression.
 */
public class RegexBytesRefFilter implements Predicate<BytesRef> {

  final private Pattern compiled;

  public RegexBytesRefFilter(String regex) {
    this.compiled = Pattern.compile(regex);
  }

  protected boolean includeString(String term) {
    Matcher m = compiled.matcher(term);
    return m.matches();
  }

  @Override
  public boolean test(BytesRef term) {
    return includeString(term.utf8ToString());
  }

}
