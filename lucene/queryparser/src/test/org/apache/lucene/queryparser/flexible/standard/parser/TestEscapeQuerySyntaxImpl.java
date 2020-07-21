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
package org.apache.lucene.queryparser.flexible.standard.parser;

import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests EscapeQuerySyntaxImpl.
 */
public class TestEscapeQuerySyntaxImpl extends LuceneTestCase {

  public void testEscapeQuoted() {
    EscapeQuerySyntax escapeQuerySyntax = new EscapeQuerySyntaxImpl();
    CharSequence sequence = escapeQuerySyntax.escape("Ipone", Locale.ROOT, EscapeQuerySyntax.Type.STRING);
    assertEquals("Ipone", sequence.toString());
    sequence = escapeQuerySyntax.escape("Ip\"on>e\"", Locale.ROOT, EscapeQuerySyntax.Type.STRING);
    assertEquals("Ip\\\"on>e\\\"", sequence.toString());
    sequence = escapeQuerySyntax.escape("İpone\"", Locale.ROOT, EscapeQuerySyntax.Type.STRING);
    assertEquals("İpone\\\"", sequence.toString());
    sequence = escapeQuerySyntax.escape("İpone ", Locale.ROOT, EscapeQuerySyntax.Type.STRING);
    assertEquals("İpone ", sequence.toString());
  }

  public void testEscapeTerm() {
    EscapeQuerySyntax escapeQuerySyntax = new EscapeQuerySyntaxImpl();
    CharSequence sequence = escapeQuerySyntax.escape("Ipone", Locale.ROOT, EscapeQuerySyntax.Type.NORMAL);
    assertEquals("Ipone", sequence.toString());
    sequence = escapeQuerySyntax.escape("Ip\"on>e\"", Locale.ROOT, EscapeQuerySyntax.Type.NORMAL);
    assertEquals("Ip\\\"on\\>e\\\"", sequence.toString());
    sequence = escapeQuerySyntax.escape("İpone\"", Locale.ROOT, EscapeQuerySyntax.Type.NORMAL);
    assertEquals("İpone\\\"", sequence.toString());
    sequence = escapeQuerySyntax.escape("İpone ", Locale.ROOT, EscapeQuerySyntax.Type.NORMAL);
    assertEquals("İpone\\ ", sequence.toString());
  }
}
