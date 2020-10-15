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

package org.apache.lucene.search.uhighlight;

import org.apache.lucene.util.LuceneTestCase;

public class TestDefaultPassageFormatter extends LuceneTestCase {
  public void testBasic() throws Exception {
    String text = "Test customization & <div class=\"xy\">&quot;escaping&quot;</div> of this very formatter. Unrelated part. It's not very N/A!";
    // fabricate passages with matches to format
    Passage[] passages = new Passage[2];
    passages[0] = new Passage();
    passages[0].setStartOffset(0);
    passages[0].setEndOffset(text.indexOf(".")+1);
    passages[0].addMatch(text.indexOf("very"), text.indexOf("very")+4, null, 2);
    passages[1] = new Passage();
    passages[1].setStartOffset(text.indexOf(".", passages[0].getEndOffset()+1) + 2);
    passages[1].setEndOffset(text.length());
    passages[1].addMatch(
        text.indexOf("very", passages[0].getEndOffset()),
        text.indexOf("very", passages[0].getEndOffset())+4, null, 2);

    // test default
    DefaultPassageFormatter formatter = new DefaultPassageFormatter();
    assertEquals(
        "Test customization & <div class=\"xy\">&quot;escaping&quot;</div> of this <b>very</b> formatter." +
            "... It's not <b>very</b> N/A!", formatter.format(passages, text));

    // test customization and encoding
    formatter = new DefaultPassageFormatter("<u>", "</u>", "\u2026 ", true);
    assertEquals(
        "Test customization &amp; &lt;div class=&quot;xy&quot;&gt;&amp;quot;escaping&amp;quot;" +
            "&lt;&#x2F;div&gt; of this <u>very</u> formatter.\u2026 It&#x27;s not <u>very</u> N&#x2F;A!",
        formatter.format(passages, text));
  }
}
