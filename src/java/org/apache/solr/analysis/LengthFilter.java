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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.IOException;

/**
 * @version $Id$
 */
public final class LengthFilter extends TokenFilter {
  final int min,max;

  public LengthFilter(TokenStream in, int min, int max) {
    super(in);
    this.min=min;
    this.max=max;
    //System.out.println("min="+min+" max="+max);
  }

  public final Token next(Token in) throws IOException {
    for (Token token=input.next(in); token!=null; token=input.next(in)) {
      final int len = token.endOffset() - token.startOffset();
      if (len<min || len>max) continue;
      return token;
    }
    return null;
  }
}
