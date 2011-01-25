package org.apache.lucene.queryParser.core.builders;

import junit.framework.Assert;

import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.util.UnescapedCharSequence;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestQueryTreeBuilder extends LuceneTestCase {
  
  @Test
  public void testSetFieldBuilder() throws QueryNodeException {
    QueryTreeBuilder qtb = new QueryTreeBuilder();
    qtb.setBuilder("field", new DummyBuilder());
    Object result = qtb.build(new FieldQueryNode(new UnescapedCharSequence("field"), "foo", 0, 0));
    Assert.assertEquals("OK", result);
    
  }
  
  private static class DummyBuilder implements QueryBuilder {

    public Object build(QueryNode queryNode) throws QueryNodeException {
      return "OK";
    }
    
  }

}
