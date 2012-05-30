package org.apache.lucene.search.spell;

public class CombineSuggestion {
  /**
   * <p>The indexes from the passed-in array of terms used to make this word combination</p>
   */
  public final int[] originalTermIndexes;
  /**
   * <p>The word combination suggestion</p>
   */
  public final SuggestWord suggestion;
  
  public CombineSuggestion (SuggestWord suggestion, int[] originalTermIndexes) {
    this.suggestion = suggestion;
    this.originalTermIndexes = originalTermIndexes;
  }  
}
