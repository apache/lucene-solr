package org.apache.lucene.search.uhighlight;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

public abstract class AnalysisOffsetStrategy extends FieldOffsetStrategy {

  protected final Analyzer analyzer;

  public AnalysisOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer analyzer) {
    super(field, queryTerms, phraseHelper, automata);
    this.analyzer = analyzer;
  }

  protected TokenStream tokenStream(String content) throws IOException {
    return MultiValueTokenStream.wrap(field, analyzer, content, UnifiedHighlighter.MULTIVAL_SEP_CHAR);
  }

  @Override
  final public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.ANALYSIS;
  }

}
