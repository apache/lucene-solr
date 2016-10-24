package org.apache.lucene.search.uhighlight;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

public class TokenStreamOffsetStrategy extends AnalysisOffsetStrategy {

  private static final BytesRef[] ZERO_LEN_BYTES_REF_ARRAY = new BytesRef[0];

  public TokenStreamOffsetStrategy(String field, BytesRef[] terms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer indexAnalyzer) {
    super(field, terms, phraseHelper, automata, indexAnalyzer);
    this.automata = convertTermsToAutomata(terms, automata);
    this.terms = ZERO_LEN_BYTES_REF_ARRAY;
  }

  @Override
  public List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException {
    TokenStream tokenStream = tokenStream(content);
    PostingsEnum mtqPostingsEnum = MultiTermHighlighting.getDocsEnum(tokenStream, automata);
    assert mtqPostingsEnum instanceof Closeable; // FYI we propagate close() later.
    mtqPostingsEnum.advance(docId);
    return Collections.singletonList(new OffsetsEnum(null, mtqPostingsEnum));
  }

  private static CharacterRunAutomaton[] convertTermsToAutomata(BytesRef[] terms, CharacterRunAutomaton[] automata) {
    CharacterRunAutomaton[] newAutomata = new CharacterRunAutomaton[terms.length + automata.length];
    for (int i = 0; i < terms.length; i++) {
      newAutomata[i] = MultiTermHighlighting.makeStringMatchAutomata(terms[i]);
    }
    // Append existing automata (that which is used for MTQs)
    System.arraycopy(automata, 0, newAutomata, terms.length, automata.length);
    return newAutomata;
  }

}
