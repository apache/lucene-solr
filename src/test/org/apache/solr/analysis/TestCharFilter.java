package org.apache.solr.analysis;

import java.io.StringReader;

import junit.framework.TestCase;

public class TestCharFilter extends TestCase {

  public void testCharFilter1() throws Exception {
    CharStream cs = new CharFilter1( new CharReader( new StringReader("") ) );
    assertEquals( "corrected position is invalid", 1, cs.correctOffset( 0 ) );
  }

  public void testCharFilter2() throws Exception {
    CharStream cs = new CharFilter2( new CharReader( new StringReader("") ) );
    assertEquals( "corrected position is invalid", 2, cs.correctOffset( 0 ) );
  }

  public void testCharFilter12() throws Exception {
    CharStream cs = new CharFilter2( new CharFilter1( new CharReader( new StringReader("") ) ) );
    assertEquals( "corrected position is invalid", 3, cs.correctOffset( 0 ) );
  }

  public void testCharFilter11() throws Exception {
    CharStream cs = new CharFilter1( new CharFilter1( new CharReader( new StringReader("") ) ) );
    assertEquals( "corrected position is invalid", 2, cs.correctOffset( 0 ) );
  }

  static class CharFilter1 extends CharFilter {

    protected CharFilter1(CharStream in) {
      super(in);
    }

    @Override
    protected int correctPosition(int currentPos) {
      return currentPos + 1;
    }
  }

  static class CharFilter2 extends CharFilter {

    protected CharFilter2(CharStream in) {
      super(in);
    }

    @Override
    protected int correctPosition(int currentPos) {
      return currentPos + 2;
    }
  }
}
