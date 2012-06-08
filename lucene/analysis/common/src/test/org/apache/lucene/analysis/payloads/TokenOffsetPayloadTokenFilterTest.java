package org.apache.lucene.analysis.payloads;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.StringReader;

public class TokenOffsetPayloadTokenFilterTest extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TokenOffsetPayloadTokenFilter nptf = new TokenOffsetPayloadTokenFilter(new MockTokenizer(new StringReader(test), MockTokenizer.WHITESPACE, false));
    int count = 0;
    PayloadAttribute payloadAtt = nptf.getAttribute(PayloadAttribute.class);
    OffsetAttribute offsetAtt = nptf.getAttribute(OffsetAttribute.class);
    nptf.reset();
    while (nptf.incrementToken()) {
      BytesRef pay = payloadAtt.getPayload();
      assertTrue("pay is null and it shouldn't be", pay != null);
      byte [] data = pay.bytes;
      int start = PayloadHelper.decodeInt(data, 0);
      assertTrue(start + " does not equal: " + offsetAtt.startOffset(), start == offsetAtt.startOffset());
      int end = PayloadHelper.decodeInt(data, 4);
      assertTrue(end + " does not equal: " + offsetAtt.endOffset(), end == offsetAtt.endOffset());
      count++;
    }
    assertTrue(count + " does not equal: " + 10, count == 10);

  }


}