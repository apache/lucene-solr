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
package org.apache.solr.legacy;


import java.io.IOException;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.solr.legacy.LegacyNumericTokenStream.LegacyNumericTermAttribute;

/** test tokenstream reuse by DefaultIndexingChain */
public class TestLegacyFieldReuse extends BaseTokenStreamTestCase {
  
  public void testNumericReuse() throws IOException {
    LegacyIntField legacyIntField = new LegacyIntField("foo", 5, Field.Store.NO);
    
    // passing null
    TokenStream ts = legacyIntField.tokenStream(null, null);
    assertTrue(ts instanceof LegacyNumericTokenStream);
    assertEquals(LegacyNumericUtils.PRECISION_STEP_DEFAULT_32, ((LegacyNumericTokenStream)ts).getPrecisionStep());
    assertNumericContents(5, ts);

    // now reuse previous stream
    legacyIntField = new LegacyIntField("foo", 20, Field.Store.NO);
    TokenStream ts2 = legacyIntField.tokenStream(null, ts);
    assertSame(ts, ts2);
    assertNumericContents(20, ts);
    
    // pass a bogus stream and ensure it's still ok
    legacyIntField = new LegacyIntField("foo", 2343, Field.Store.NO);
    TokenStream bogus = new CannedTokenStream(new Token("bogus", 0, 5));
    ts = legacyIntField.tokenStream(null, bogus);
    assertNotSame(bogus, ts);
    assertNumericContents(2343, ts);
    
    // pass another bogus stream (numeric, but different precision step!)
    legacyIntField = new LegacyIntField("foo", 42, Field.Store.NO);
    assert 3 != LegacyNumericUtils.PRECISION_STEP_DEFAULT;
    bogus = new LegacyNumericTokenStream(3);
    ts = legacyIntField.tokenStream(null, bogus);
    assertNotSame(bogus, ts);
    assertNumericContents(42, ts);
  }
   
  private void assertNumericContents(int value, TokenStream ts) throws IOException {
    assertTrue(ts instanceof LegacyNumericTokenStream);
    LegacyNumericTermAttribute numericAtt = ts.getAttribute(LegacyNumericTermAttribute.class);
    ts.reset();
    boolean seen = false;
    while (ts.incrementToken()) {
      if (numericAtt.getShift() == 0) {
        assertEquals(value, numericAtt.getRawValue());
        seen = true;
      }
    }
    ts.end();
    ts.close();
    assertTrue(seen);
  }
}
