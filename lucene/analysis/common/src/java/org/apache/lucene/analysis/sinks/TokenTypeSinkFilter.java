package org.apache.lucene.analysis.sinks;

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

import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * Adds a token to the sink if it has a specific type.
 */
public class TokenTypeSinkFilter extends TeeSinkTokenFilter.SinkFilter {
  private String typeToMatch;
  private TypeAttribute typeAtt;

  public TokenTypeSinkFilter(String typeToMatch) {
    this.typeToMatch = typeToMatch;
  }

  @Override
  public boolean accept(AttributeSource source) {
    if (typeAtt == null) {
      typeAtt = source.addAttribute(TypeAttribute.class);
    }
    
    //check to see if this is a Category
    return (typeToMatch.equals(typeAtt.type()));
  }

}
