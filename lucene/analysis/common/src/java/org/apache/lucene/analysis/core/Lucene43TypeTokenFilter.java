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
package org.apache.lucene.analysis.core;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.Lucene43FilteringTokenFilter;

import java.io.IOException;
import java.util.Set;

/**
 * Backcompat TypeTokenFilter for versions 4.3 and before.
 * @deprecated Use {@link org.apache.lucene.analysis.core.TypeTokenFilter}
 */
@Deprecated
public final class Lucene43TypeTokenFilter extends Lucene43FilteringTokenFilter {

  private final Set<String> stopTypes;
  private final TypeAttribute typeAttribute = addAttribute(TypeAttribute.class);
  private final boolean useWhiteList;

  public Lucene43TypeTokenFilter(boolean enablePositionIncrements, TokenStream input, Set<String> stopTypes, boolean useWhiteList) {
    super(enablePositionIncrements, input);
    this.stopTypes = stopTypes;
    this.useWhiteList = useWhiteList;
  }

  /**
   * By default accept the token if its type is not a stop type.
   * When the useWhiteList parameter is set to true then accept the token if its type is contained in the stopTypes
   */
  @Override
  protected boolean accept() throws IOException {
    return useWhiteList == stopTypes.contains(typeAttribute.type());
  }
}
