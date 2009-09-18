package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.index.Payload;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.tokenattributes.*;

/** This class tests some special cases of backwards compatibility when using the new TokenStream API with old analyzers */
public class TestTokenStreamBWComp extends LuceneTestCase {

  private static final String doc = "This is the new TokenStream api";
  private static final String[] stopwords = new String[] {"is", "the", "this"};
  private static final String[] results = new String[] {"new", "tokenstream", "api"};

  public static class POSToken extends Token {
    public static final int PROPERNOUN = 1;
    public static final int NO_NOUN = 2;
    
    private int partOfSpeech;
    
    public void setPartOfSpeech(int pos) {
      partOfSpeech = pos;
    }
    
    public int getPartOfSpeech() {
      return this.partOfSpeech;
    }
  }
  
  static class PartOfSpeechTaggingFilter extends TokenFilter {

    protected PartOfSpeechTaggingFilter(TokenStream input) {
      super(input);
    }
    
    public Token next() throws IOException {
      Token t = input.next();
      if (t == null) return null;
      
      POSToken pt = new POSToken();
      pt.reinit(t);
      if (pt.termLength() > 0) {
        if (Character.isUpperCase(pt.termBuffer()[0])) {
          pt.setPartOfSpeech(POSToken.PROPERNOUN);
        } else {
          pt.setPartOfSpeech(POSToken.NO_NOUN);
        }
      }
      return pt;
    }
    
  }

  static class PartOfSpeechAnnotatingFilter extends TokenFilter {
    public final static byte PROPER_NOUN_ANNOTATION = 1;
    
    
    protected PartOfSpeechAnnotatingFilter(TokenStream input) {
      super(input);
    }
    
    public Token next() throws IOException {
      Token t = input.next();
      if (t == null) return null;
      
      if (t instanceof POSToken) {
        POSToken pt = (POSToken) t;
        if (pt.getPartOfSpeech() == POSToken.PROPERNOUN) {
          pt.setPayload(new Payload(new byte[] {PROPER_NOUN_ANNOTATION}));
        }
        return pt;
      } else {
        return t;
      }
    }
    
  }

  // test the chain: The one and only term "TokenStream" should be declared as proper noun:

  public void testTeeSinkCustomTokenNewAPI() throws IOException {
    testTeeSinkCustomToken(0);
  }
  
  public void testTeeSinkCustomTokenOldAPI() throws IOException {
    testTeeSinkCustomToken(1);
  }

  public void testTeeSinkCustomTokenVeryOldAPI() throws IOException {
    testTeeSinkCustomToken(2);
  }

  private void testTeeSinkCustomToken(int api) throws IOException {
    TokenStream stream = new WhitespaceTokenizer(new StringReader(doc));
    stream = new PartOfSpeechTaggingFilter(stream);
    stream = new LowerCaseFilter(stream);
    stream = new StopFilter(stream, stopwords);
    
    SinkTokenizer sink = new SinkTokenizer();
    TokenStream stream1 = new PartOfSpeechAnnotatingFilter(sink);
    
    stream = new TeeTokenFilter(stream, sink);
    stream = new PartOfSpeechAnnotatingFilter(stream);

    switch (api) {
      case 0:
        consumeStreamNewAPI(stream);
        consumeStreamNewAPI(stream1);
        break;
      case 1:
        consumeStreamOldAPI(stream);
        consumeStreamOldAPI(stream1);
        break;
      case 2:
        consumeStreamVeryOldAPI(stream);
        consumeStreamVeryOldAPI(stream1);
        break;
    }
  }

  // test caching the special custom POSToken works in all cases

  public void testCachingCustomTokenNewAPI() throws IOException {
    testTeeSinkCustomToken(0);
  }
  
  public void testCachingCustomTokenOldAPI() throws IOException {
    testTeeSinkCustomToken(1);
  }

  public void testCachingCustomTokenVeryOldAPI() throws IOException {
    testTeeSinkCustomToken(2);
  }

  public void testCachingCustomTokenMixed() throws IOException {
    testTeeSinkCustomToken(3);
  }

  private void testCachingCustomToken(int api) throws IOException {
    TokenStream stream = new WhitespaceTokenizer(new StringReader(doc));
    stream = new PartOfSpeechTaggingFilter(stream);
    stream = new LowerCaseFilter(stream);
    stream = new StopFilter(stream, stopwords);
    stream = new CachingTokenFilter(stream); // <- the caching is done before the annotating!
    stream = new PartOfSpeechAnnotatingFilter(stream);

    switch (api) {
      case 0:
        consumeStreamNewAPI(stream);
        consumeStreamNewAPI(stream);
        break;
      case 1:
        consumeStreamOldAPI(stream);
        consumeStreamOldAPI(stream);
        break;
      case 2:
        consumeStreamVeryOldAPI(stream);
        consumeStreamVeryOldAPI(stream);
        break;
      case 3:
        consumeStreamNewAPI(stream);
        consumeStreamOldAPI(stream);
        consumeStreamVeryOldAPI(stream);
        consumeStreamNewAPI(stream);
        consumeStreamVeryOldAPI(stream);
        break;
    }
  }

  private static void consumeStreamNewAPI(TokenStream stream) throws IOException {
    stream.reset();
    PayloadAttribute payloadAtt = (PayloadAttribute) stream.addAttribute(PayloadAttribute.class);
    TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
    
    int i=0;
    while (stream.incrementToken()) {
      String term = termAtt.term();
      Payload p = payloadAtt.getPayload();
      if (p != null && p.getData().length == 1 && p.getData()[0] == PartOfSpeechAnnotatingFilter.PROPER_NOUN_ANNOTATION) {
        assertEquals("only TokenStream is a proper noun", "tokenstream", term);
      } else {
        assertFalse("all other tokens (if this test fails, the special POSToken subclass is not correctly passed through the chain)", "tokenstream".equals(term));
      }
      assertEquals(results[i], term);
      i++;
    }   
  }

  private static void consumeStreamOldAPI(TokenStream stream) throws IOException {
    stream.reset();
    Token reusableToken = new Token();
    
    int i=0;
    while ((reusableToken = stream.next(reusableToken)) != null) {
      String term = reusableToken.term();
      Payload p = reusableToken.getPayload();
      if (p != null && p.getData().length == 1 && p.getData()[0] == PartOfSpeechAnnotatingFilter.PROPER_NOUN_ANNOTATION) {
        assertEquals("only TokenStream is a proper noun", "tokenstream", term);
      } else {
        assertFalse("all other tokens (if this test fails, the special POSToken subclass is not correctly passed through the chain)", "tokenstream".equals(term));
      }
      assertEquals(results[i], term);
      i++;
    }   
  }

  private static void consumeStreamVeryOldAPI(TokenStream stream) throws IOException {
    stream.reset();
    
    Token token;
    int i=0;
    while ((token = stream.next()) != null) {
      String term = token.term();
      Payload p = token.getPayload();
      if (p != null && p.getData().length == 1 && p.getData()[0] == PartOfSpeechAnnotatingFilter.PROPER_NOUN_ANNOTATION) {
        assertEquals("only TokenStream is a proper noun", "tokenstream", term);
      } else {
        assertFalse("all other tokens (if this test fails, the special POSToken subclass is not correctly passed through the chain)", "tokenstream".equals(term));
      }
      assertEquals(results[i], term);
      i++;
    }   
  }
  
  // test if tokenization fails, if only the new API is allowed and an old TokenStream is in the chain
  public void testOnlyNewAPI() throws IOException {
    TokenStream.setOnlyUseNewAPI(true);
    try {
    
      // this should fail with UOE
      try {
        TokenStream stream = new WhitespaceTokenizer(new StringReader(doc));
        stream = new PartOfSpeechTaggingFilter(stream); // <-- this one is evil!
        stream = new LowerCaseFilter(stream);
        stream = new StopFilter(stream, stopwords);
        while (stream.incrementToken());
        fail("If only the new API is allowed, this should fail with an UOE");
      } catch (UnsupportedOperationException uoe) {
        assertEquals((PartOfSpeechTaggingFilter.class.getName()+" does not implement incrementToken() which is needed for onlyUseNewAPI."),uoe.getMessage());
      }

      // this should pass, as all core token streams support the new API
      TokenStream stream = new WhitespaceTokenizer(new StringReader(doc));
      stream = new LowerCaseFilter(stream);
      stream = new StopFilter(stream, stopwords);
      while (stream.incrementToken());
      
      // Test, if all attributes are implemented by their implementation, not Token/TokenWrapper
      assertTrue("TermAttribute is not implemented by TermAttributeImpl",
        stream.addAttribute(TermAttribute.class) instanceof TermAttributeImpl);
      assertTrue("OffsetAttribute is not implemented by OffsetAttributeImpl",
        stream.addAttribute(OffsetAttribute.class) instanceof OffsetAttributeImpl);
      assertTrue("FlagsAttribute is not implemented by FlagsAttributeImpl",
        stream.addAttribute(FlagsAttribute.class) instanceof FlagsAttributeImpl);
      assertTrue("PayloadAttribute is not implemented by PayloadAttributeImpl",
        stream.addAttribute(PayloadAttribute.class) instanceof PayloadAttributeImpl);
      assertTrue("PositionIncrementAttribute is not implemented by PositionIncrementAttributeImpl", 
        stream.addAttribute(PositionIncrementAttribute.class) instanceof PositionIncrementAttributeImpl);
      assertTrue("TypeAttribute is not implemented by TypeAttributeImpl",
        stream.addAttribute(TypeAttribute.class) instanceof TypeAttributeImpl);
        
      // try to call old API, this should fail
      try {
        stream.reset();
        Token reusableToken = new Token();
        while ((reusableToken = stream.next(reusableToken)) != null);
        fail("If only the new API is allowed, this should fail with an UOE");
      } catch (UnsupportedOperationException uoe) {
        assertEquals("This TokenStream only supports the new Attributes API.", uoe.getMessage());
      }
      try {
        stream.reset();
        while (stream.next() != null);
        fail("If only the new API is allowed, this should fail with an UOE");
      } catch (UnsupportedOperationException uoe) {
        assertEquals("This TokenStream only supports the new Attributes API.", uoe.getMessage());
      }
      
      // Test if the wrapper API (onlyUseNewAPI==false) uses TokenWrapper
      // as attribute instance.
      // TokenWrapper encapsulates a Token instance that can be exchanged
      // by another Token instance without changing the AttributeImpl instance
      // itsself.
      TokenStream.setOnlyUseNewAPI(false);
      stream = new WhitespaceTokenizer(new StringReader(doc));
      assertTrue("TermAttribute is not implemented by TokenWrapper",
        stream.addAttribute(TermAttribute.class) instanceof TokenWrapper);
      assertTrue("OffsetAttribute is not implemented by TokenWrapper",
        stream.addAttribute(OffsetAttribute.class) instanceof TokenWrapper);
      assertTrue("FlagsAttribute is not implemented by TokenWrapper",
        stream.addAttribute(FlagsAttribute.class) instanceof TokenWrapper);
      assertTrue("PayloadAttribute is not implemented by TokenWrapper",
        stream.addAttribute(PayloadAttribute.class) instanceof TokenWrapper);
      assertTrue("PositionIncrementAttribute is not implemented by TokenWrapper",
        stream.addAttribute(PositionIncrementAttribute.class) instanceof TokenWrapper);
      assertTrue("TypeAttribute is not implemented by TokenWrapper",
        stream.addAttribute(TypeAttribute.class) instanceof TokenWrapper);
      
    } finally {
      TokenStream.setOnlyUseNewAPI(false);
    }
  }
  
  public void testOverridesAny() throws Exception {
    try {
      TokenStream stream = new WhitespaceTokenizer(new StringReader(doc));
      stream = new TokenFilter(stream) {
        // we implement nothing, only un-abstract it
      };
      stream = new LowerCaseFilter(stream);
      stream = new StopFilter(stream, stopwords);
      while (stream.incrementToken());
      fail("One TokenFilter does not override any of the required methods, so it should fail.");
    } catch (UnsupportedOperationException uoe) {
      assertTrue("invalid UOE message", uoe.getMessage().endsWith("does not implement any of incrementToken(), next(Token), next()."));
    }
  }
  
  public void testMixedOldApiConsumer() throws Exception {
    // WhitespaceTokenizer is using incrementToken() API:
    TokenStream stream = new WhitespaceTokenizer(new StringReader("foo bar moo maeh"));
    
    Token foo = new Token();
    foo = stream.next(foo);
    Token bar = stream.next();
    assertEquals("foo", foo.term());
    assertEquals("bar", bar.term());
    
    Token moo = stream.next(foo);
    assertEquals("moo", moo.term());
    assertEquals("private 'bar' term should still be valid", "bar", bar.term());
    
    // and now we also use incrementToken()... (very bad, but should work)
    TermAttribute termAtt = (TermAttribute) stream.getAttribute(TermAttribute.class);
    assertTrue(stream.incrementToken());
    assertEquals("maeh", termAtt.term());    
    assertEquals("private 'bar' term should still be valid", "bar", bar.term());    
  }
  
  /*
   * old api that cycles thru foo, bar, meh
   */
  private class RoundRobinOldAPI extends TokenStream {
    int count = 0;
    String terms[] = { "foo", "bar", "meh" };

    public Token next(Token reusableToken) throws IOException {
      reusableToken.setTermBuffer(terms[count % terms.length]);
      count++;
      return reusableToken;
    }
  }
  
  public void testMixedOldApiConsumer2() throws Exception {
    // RoundRobinOldAPI is using TokenStream(next)
    TokenStream stream = new RoundRobinOldAPI();
    TermAttribute termAtt = (TermAttribute) stream.getAttribute(TermAttribute.class);
    
    assertTrue(stream.incrementToken());
    Token bar = stream.next();
    assertEquals("foo", termAtt.term());
    assertEquals("bar", bar.term());

    assertTrue(stream.incrementToken());
    assertEquals("meh", termAtt.term());
    assertEquals("private 'bar' term should still be valid", "bar", bar.term());

    Token foo = stream.next();
    assertEquals("the term attribute should still be the same", "meh", termAtt.term());
    assertEquals("foo", foo.term());
    assertEquals("private 'bar' term should still be valid", "bar", bar.term());
  }
  
}
