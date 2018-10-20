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
package org.apache.lucene.analysis.payloads;


import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;


/**
 * Makes the {@link TypeAttribute} a payload.
 *
 * Encodes the type using {@link String#getBytes(String)} with "UTF-8" as the encoding
 *
 **/
public class TypeAsPayloadTokenFilter extends TokenFilter {
  private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  public TypeAsPayloadTokenFilter(TokenStream input) {
    super(input);
  }


  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      String type = typeAtt.type();
      if (type != null && !type.isEmpty()) {
        payloadAtt.setPayload(new BytesRef(type));
      }
      return true;
    } else {
      return false;
    }
  }
}