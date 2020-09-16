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
package org.apache.lucene.queryparser.flexible.standard;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.queryparser.flexible.standard.parser.FastCharStream;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

/**
 * Sanity checks for standard syntax parser.
 */
public class TestStandardSyntaxParser extends LuceneTestCase {
  @Test
  public void testClause() throws Exception {
    StandardSyntaxParser qp = new StandardSyntaxParser(new FastCharStream(Reader.nullReader()));
    for (String in : Arrays.asList(
        "field~2",
        "/field/~2",
        "/field/",
        "10",
        "10.1"
        /*
        "term1",
        "field=term",
        "field:term",
        "field:(term1 term2)",
        "field:(term1 term2)^2",
        "(field1:term1 field2:term2 term3)",
        "field<term",
        "field<=term",
        "field>term",
        "field>=term",
        "field>2",
        "field<2",
        "field<\"term\""
         */
    )) {
      System.out.println("# " + in);
      qp.ReInit(new FastCharStream(new StringReader(in)));
      QueryNode node = qp.Clause("_def_");
      System.out.println(in + " => " + node.toQueryString(new EscapeQuerySyntaxImpl()));
      System.out.println(in + " => " + node.toString());
    }
  }

  // Previously invalid combinations
  // "/field/~2",
}
