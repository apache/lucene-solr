package org.apache.lucene.queryParser.standard.processors;

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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryParser.core.nodes.GroupQueryNode;
import org.apache.lucene.queryParser.core.nodes.NoTokenFoundQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.TextableQueryNode;
import org.apache.lucene.queryParser.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryParser.standard.config.AnalyzerAttribute;
import org.apache.lucene.queryParser.standard.config.PositionIncrementsAttribute;
import org.apache.lucene.queryParser.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryParser.standard.nodes.StandardBooleanQueryNode;
import org.apache.lucene.queryParser.standard.nodes.WildcardQueryNode;

/**
 * This processor verifies if the attribute {@link AnalyzerQueryNodeProcessor}
 * is defined in the {@link QueryConfigHandler}. If it is and the analyzer is
 * not <code>null</code>, it looks for every {@link FieldQueryNode} that is not
 * {@link WildcardQueryNode}, {@link FuzzyQueryNode} or
 * {@link ParametricQueryNode} contained in the query node tree, then it applies
 * the analyzer to that {@link FieldQueryNode} object. <br/>
 * <br/>
 * If the analyzer return only one term, the returned term is set to the
 * {@link FieldQueryNode} and it's returned. <br/>
 * <br/>
 * If the analyzer return more than one term, a {@link TokenizedPhraseQueryNode}
 * or {@link MultiPhraseQueryNode} is created, whether there is one or more
 * terms at the same position, and it's returned. <br/>
 * <br/>
 * If no term is returned by the analyzer a {@link NoTokenFoundQueryNode} object
 * is returned. <br/>
 * 
 * @see Analyzer
 * @see TokenStream
 */
public class AnalyzerQueryNodeProcessor extends QueryNodeProcessorImpl {

  private Analyzer analyzer;

  private boolean positionIncrementsEnabled;

  public AnalyzerQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {

    if (getQueryConfigHandler().hasAttribute(AnalyzerAttribute.class)) {

      this.analyzer = getQueryConfigHandler().getAttribute(
          AnalyzerAttribute.class).getAnalyzer();

      this.positionIncrementsEnabled = false;

      if (getQueryConfigHandler().hasAttribute(
          PositionIncrementsAttribute.class)) {

        if (getQueryConfigHandler().getAttribute(
            PositionIncrementsAttribute.class).isPositionIncrementsEnabled()) {

          this.positionIncrementsEnabled = true;

        }

      }

      if (this.analyzer != null) {
        return super.process(queryTree);
      }

    }

    return queryTree;

  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof TextableQueryNode
        && !(node instanceof WildcardQueryNode)
        && !(node instanceof FuzzyQueryNode)
        && !(node instanceof ParametricQueryNode)) {

      FieldQueryNode fieldNode = ((FieldQueryNode) node);
      String text = fieldNode.getTextAsString();
      String field = fieldNode.getFieldAsString();

      TokenStream source;
      try {
        source = this.analyzer.reusableTokenStream(field, new StringReader(text));
        source.reset();
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
      CachingTokenFilter buffer = new CachingTokenFilter(source);

      PositionIncrementAttribute posIncrAtt = null;
      int numTokens = 0;
      int positionCount = 0;
      boolean severalTokensAtSamePosition = false;

      if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
        posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
      }

      try {

        while (buffer.incrementToken()) {
          numTokens++;
          int positionIncrement = (posIncrAtt != null) ? posIncrAtt
              .getPositionIncrement() : 1;
          if (positionIncrement != 0) {
            positionCount += positionIncrement;

          } else {
            severalTokensAtSamePosition = true;
          }

        }

      } catch (IOException e) {
        // ignore
      }

      try {
        // rewind the buffer stream
        buffer.reset();

        // close original stream - all tokens buffered
        source.close();
      } catch (IOException e) {
        // ignore
      }

      if (!buffer.hasAttribute(CharTermAttribute.class)) {
        return new NoTokenFoundQueryNode();
      }

      CharTermAttribute termAtt = buffer.getAttribute(CharTermAttribute.class);

      if (numTokens == 0) {
        return new NoTokenFoundQueryNode();

      } else if (numTokens == 1) {
        String term = null;
        try {
          boolean hasNext;
          hasNext = buffer.incrementToken();
          assert hasNext == true;
          term = termAtt.toString();

        } catch (IOException e) {
          // safe to ignore, because we know the number of tokens
        }

        fieldNode.setText(term);

        return fieldNode;

      } else if (severalTokensAtSamePosition || !(node instanceof QuotedFieldQueryNode)) {
        if (positionCount == 1 || !(node instanceof QuotedFieldQueryNode)) {
          // no phrase query:
          LinkedList<QueryNode> children = new LinkedList<QueryNode>();

          for (int i = 0; i < numTokens; i++) {
            String term = null;
            try {
              boolean hasNext = buffer.incrementToken();
              assert hasNext == true;
              term = termAtt.toString();

            } catch (IOException e) {
              // safe to ignore, because we know the number of tokens
            }

            children.add(new FieldQueryNode(field, term, -1, -1));

          }
          if (positionCount == 1)
            return new GroupQueryNode(
              new StandardBooleanQueryNode(children, true));
          else
            return new StandardBooleanQueryNode(children, false);

        } else {
          // phrase query:
          MultiPhraseQueryNode mpq = new MultiPhraseQueryNode();

          List<FieldQueryNode> multiTerms = new ArrayList<FieldQueryNode>();
          int position = -1;
          int i = 0;
          int termGroupCount = 0;
          for (; i < numTokens; i++) {
            String term = null;
            int positionIncrement = 1;
            try {
              boolean hasNext = buffer.incrementToken();
              assert hasNext == true;
              term = termAtt.toString();
              if (posIncrAtt != null) {
                positionIncrement = posIncrAtt.getPositionIncrement();
              }

            } catch (IOException e) {
              // safe to ignore, because we know the number of tokens
            }

            if (positionIncrement > 0 && multiTerms.size() > 0) {

              for (FieldQueryNode termNode : multiTerms) {

                if (this.positionIncrementsEnabled) {
                  termNode.setPositionIncrement(position);
                } else {
                  termNode.setPositionIncrement(termGroupCount);
                }

                mpq.add(termNode);

              }

              // Only increment once for each "group" of
              // terms that were in the same position:
              termGroupCount++;

              multiTerms.clear();

            }

            position += positionIncrement;
            multiTerms.add(new FieldQueryNode(field, term, -1, -1));

          }

          for (FieldQueryNode termNode : multiTerms) {

            if (this.positionIncrementsEnabled) {
              termNode.setPositionIncrement(position);

            } else {
              termNode.setPositionIncrement(termGroupCount);
            }

            mpq.add(termNode);

          }

          return mpq;

        }

      } else {

        TokenizedPhraseQueryNode pq = new TokenizedPhraseQueryNode();

        int position = -1;

        for (int i = 0; i < numTokens; i++) {
          String term = null;
          int positionIncrement = 1;

          try {
            boolean hasNext = buffer.incrementToken();
            assert hasNext == true;
            term = termAtt.toString();

            if (posIncrAtt != null) {
              positionIncrement = posIncrAtt.getPositionIncrement();
            }

          } catch (IOException e) {
            // safe to ignore, because we know the number of tokens
          }

          FieldQueryNode newFieldNode = new FieldQueryNode(field, term, -1, -1);

          if (this.positionIncrementsEnabled) {
            position += positionIncrement;
            newFieldNode.setPositionIncrement(position);

          } else {
            newFieldNode.setPositionIncrement(i);
          }

          pq.add(newFieldNode);

        }

        return pq;

      }

    }

    return node;

  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}
