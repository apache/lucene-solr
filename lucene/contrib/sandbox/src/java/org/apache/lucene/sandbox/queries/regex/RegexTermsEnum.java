package org.apache.lucene.sandbox.queries.regex;

/**
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

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified regular expression term using the specified regular expression
 * implementation.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */

public class RegexTermsEnum extends FilteredTermsEnum {

  private RegexCapabilities.RegexMatcher regexImpl;
  private final BytesRef prefixRef;

  public RegexTermsEnum(TermsEnum tenum, Term term, RegexCapabilities regexCap) throws IOException {
    super(tenum);
    String text = term.text();
    this.regexImpl = regexCap.compile(text);

    String pre = regexImpl.prefix();
    if (pre == null) {
      pre = "";
    }

    setInitialSeekTerm(prefixRef = new BytesRef(pre));
  }

  @Override
  protected AcceptStatus accept(BytesRef term) {
    if (StringHelper.startsWith(term, prefixRef)) {
      // TODO: set BoostAttr based on distance of
      // searchTerm.text() and term().text()
      return regexImpl.match(term) ? AcceptStatus.YES : AcceptStatus.NO;
    } else {
      return AcceptStatus.NO;
    }
  }
}
