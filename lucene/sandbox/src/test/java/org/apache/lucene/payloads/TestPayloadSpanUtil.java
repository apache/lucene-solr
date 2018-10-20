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
package org.apache.lucene.payloads;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestPayloadSpanUtil extends LuceneTestCase {

  public static final String FIELD = "f";

  public void testPayloadSpanUtil() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new PayloadAnalyzer()).setSimilarity(new ClassicSimilarity()));

    Document doc = new Document();
    doc.add(newTextField(FIELD, "xx rr yy mm  pp", Field.Store.YES));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    PayloadSpanUtil psu = new PayloadSpanUtil(searcher.getTopReaderContext());

    Collection<byte[]> payloads = psu.getPayloadsForQuery(new TermQuery(new Term(FIELD, "rr")));
    if(VERBOSE) {
      System.out.println("Num payloads:" + payloads.size());
      for (final byte [] bytes : payloads) {
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
      }
    }
    reader.close();
    directory.close();
  }

  final static class PayloadAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result));
    }
  }

  static final class PayloadFilter extends TokenFilter {
    Set<String> entities = new HashSet<>();
    Set<String> nopayload = new HashSet<>();
    int pos;
    PayloadAttribute payloadAtt;
    CharTermAttribute termAtt;
    PositionIncrementAttribute posIncrAtt;

    public PayloadFilter(TokenStream input) {
      super(input);
      pos = 0;
      entities.add("xx");
      entities.add("one");
      nopayload.add("nopayload");
      nopayload.add("np");
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      payloadAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        String token = termAtt.toString();

        if (!nopayload.contains(token)) {
          if (entities.contains(token)) {
            payloadAtt.setPayload(new BytesRef(token + ":Entity:"+ pos ));
          } else {
            payloadAtt.setPayload(new BytesRef(token + ":Noise:" + pos ));
          }
        }
        pos += posIncrAtt.getPositionIncrement();
        return true;
      }
      return false;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.pos = 0;
    }
  }

}
