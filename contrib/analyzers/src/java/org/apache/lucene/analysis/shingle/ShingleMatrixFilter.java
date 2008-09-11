package org.apache.lucene.analysis.shingle;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.Payload;


/**
 * <p>A ShingleMatrixFilter constructs shingles (token n-grams) from a token stream.
 * In other words, it creates combinations of tokens as a single token.
 *
 * <p>For example, the sentence "please divide this sentence into shingles"
 * might be tokenized into shingles "please divide", "divide this",
 * "this sentence", "sentence into", and "into shingles".
 *
 * <p>Using a shingle filter at index and query time can in some instances
 * be used to replace phrase queries, especially them with 0 slop.
 *
 * <p>Without a spacer character
 * it can be used to handle composition and decomposion of words
 * such as searching for "multi dimensional" instead of "multidimensional".
 * It is a rather common human problem at query time
 * in several languages, notebly the northern Germanic branch.
 *
 * <p>Shingles are amongst many things also known to solve problems
 * in spell checking, language detection and document clustering.  
 *
 * <p>This filter is backed by a three dimensional column oriented matrix
 * used to create permutations of the second dimension, the rows,
 * and leaves the third, the z-axis, for for multi token synonyms.
 *
 * <p>In order to use this filter you need to define a way of positioning
 * the input stream tokens in the matrix. This is done using a
 * {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.TokenSettingsCodec}.
 * There are three simple implementations for demonstrational purposes,
 * see {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.OneDimensionalNonWeightedTokenSettingsCodec},
 * {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.TwoDimensionalNonWeightedSynonymTokenSettingsCodec}
 * and {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.SimpleThreeDimensionalTokenSettingsCodec}.
 *
 * <p>Consider this token matrix:
 * <pre>
 *  Token[column][row][z-axis]{
 *    {{hello}, {greetings, and, salutations}},
 *    {{world}, {earth}, {tellus}}
 *  };
 * </pre>
 *
 * It would produce the following 2-3 gram sized shingles:
 *
 * <pre>
 * "hello_world"
 * "greetings_and"
 * "greetings_and_salutations"
 * "and_salutations"
 * "and_salutations_world"
 * "salutations_world"
 * "hello_earth"
 * "and_salutations_earth"
 * "salutations_earth"
 * "hello_tellus"
 * "and_salutations_tellus"
 * "salutations_tellus"
 *  </pre>
 * 
 * <p>This implementation can be rather heap demanding
 * if (maximum shingle size - minimum shingle size) is a great number and the stream contains many columns,
 * or if each column contains a great number of rows.
 *
 * <p>The problem is that in order avoid producing duplicates
 * the filter needs to keep track of any shingle already produced and returned to the consumer.
 *
 * There is a bit of resource management to handle this
 * but it would of course be much better if the filter was written
 * so it never created the same shingle more than once in the first place.
 *
 * <p>The filter also has basic support for calculating weights for the shingles
 * based on the weights of the tokens from the input stream, output shingle size, et c.
 * See {@link #calculateShingleWeight(org.apache.lucene.analysis.Token, java.util.List, int, java.util.List, java.util.List)}.
 */
public class ShingleMatrixFilter extends TokenStream {

  public static Character defaultSpacerCharacter = new Character('_');
  public static TokenSettingsCodec defaultSettingsCodec = new OneDimensionalNonWeightedTokenSettingsCodec();
  public static boolean ignoringSinglePrefixOrSuffixShingleByDefault = false;

  /**
   * Strategy used to code and decode meta data of the tokens from the input stream
   * regarding how to position the tokens in the matrix, set and retreive weight, et c.
   */
  public static abstract class TokenSettingsCodec {

    /**
     * Retrieves information on how a {@link org.apache.lucene.analysis.Token} is to be inserted to a {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix}.
     * @param token
     * @return
     * @throws IOException
     */
    public abstract TokenPositioner getTokenPositioner(Token token) throws IOException;

    /**
     * Sets information on how a {@link org.apache.lucene.analysis.Token} is to be inserted to a {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix}.
     *
     * @param token
     * @param tokenPositioner
     */
    public abstract void setTokenPositioner(Token token, ShingleMatrixFilter.TokenPositioner tokenPositioner);

    /**
     * Have this method return 1f in order to 'disable' weights.
     * @param token
     * @return the weight of parameter token
     */
    public abstract float getWeight(Token token);

    /**
     * Have this method do nothing in order to 'disable' weights.
     * @param token
     * @param weight
     */
    public abstract void setWeight(Token token, float weight);
  }


  /**
   * Used to describe how a {@link org.apache.lucene.analysis.Token} is to be inserted to a {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix}.
   * @see org.apache.lucene.analysis.shingle.ShingleMatrixFilter.TokenSettingsCodec#getTokenPositioner(org.apache.lucene.analysis.Token)
   * @see org.apache.lucene.analysis.shingle.ShingleMatrixFilter.TokenSettingsCodec#setTokenPositioner(org.apache.lucene.analysis.Token,org.apache.lucene.analysis.shingle.ShingleMatrixFilter.TokenPositioner)
   */
  public static class TokenPositioner {
    public static final TokenPositioner newColumn = new TokenPositioner(0);
    public static final TokenPositioner newRow = new TokenPositioner(1);
    public static final TokenPositioner sameRow = new TokenPositioner(2);

    private final int index;

    private TokenPositioner(int index) {
      this.index = index;
    }

    public int getIndex() {
      return index;
    }
  }

  // filter instance settings variables

  private TokenSettingsCodec settingsCodec;

  private int minimumShingleSize;
  private int maximumShingleSize;

  private boolean ignoringSinglePrefixOrSuffixShingle = false;

  private Character spacerCharacter = defaultSpacerCharacter;

  private TokenStream input;


  /**
   * Creates a shingle filter based on a user defined matrix.
   *
   * The filter /will/ delete columns from the input matrix! You will not be able to reset the filter if you used this constructor.
   * todo: don't touch the matrix! use a boolean, set the input stream to null or something, and keep track of where in the matrix we are at.
   *
   * @param matrix the input based for creating shingles. Does not need to contain any information until {@link org.apache.lucene.analysis.shingle.ShingleMatrixFilter#next(org.apache.lucene.analysis.Token)} is called the first time.
   * @param minimumShingleSize minimum number of tokens in any shingle.
   * @param maximumShingleSize maximum number of tokens in any shingle.
   * @param spacerCharacter character to use between texts of the token parts in a shingle. null for none.
   * @param ignoringSinglePrefixOrSuffixShingle if true, shingles that only contains permutation of the first of the last column will not be produced as shingles. Useful when adding boundary marker tokens such as '^' and '$'.
   * @param settingsCodec codec used to read input token weight and matrix positioning.
   */
  public ShingleMatrixFilter(Matrix matrix, int minimumShingleSize, int maximumShingleSize, Character spacerCharacter, boolean ignoringSinglePrefixOrSuffixShingle, TokenSettingsCodec settingsCodec) {
    this.matrix = matrix;
    this.minimumShingleSize = minimumShingleSize;
    this.maximumShingleSize = maximumShingleSize;
    this.spacerCharacter = spacerCharacter;
    this.ignoringSinglePrefixOrSuffixShingle = ignoringSinglePrefixOrSuffixShingle;
    this.settingsCodec = settingsCodec;

    // set the input to be an empty token stream, we already have the data.
    this.input = new EmptyTokenStream();
  }

  /**
   * Creates a shingle filter using default settings.
   *
   * @see #defaultSpacerCharacter
   * @see #ignoringSinglePrefixOrSuffixShingleByDefault
   * @see #defaultSettingsCodec
   *
   * @param input stream from wich to construct the matrix
   * @param minimumShingleSize minimum number of tokens in any shingle.
   * @param maximumShingleSize maximum number of tokens in any shingle.
   */
  public ShingleMatrixFilter(TokenStream input, int minimumShingleSize, int maximumShingleSize) {
    this(input, minimumShingleSize, maximumShingleSize, defaultSpacerCharacter);
  }


  /**
   * Creates a shingle filter using default settings.
   *
   * @see #ignoringSinglePrefixOrSuffixShingleByDefault
   * @see #defaultSettingsCodec
   *
   * @param input stream from wich to construct the matrix
   * @param minimumShingleSize minimum number of tokens in any shingle.
   * @param maximumShingleSize maximum number of tokens in any shingle.
   * @param spacerCharacter character to use between texts of the token parts in a shingle. null for none.
   */
  public ShingleMatrixFilter(TokenStream input, int minimumShingleSize, int maximumShingleSize, Character spacerCharacter) {
    this(input, minimumShingleSize, maximumShingleSize, spacerCharacter, ignoringSinglePrefixOrSuffixShingleByDefault);
  }

  /**
   * Creates a shingle filter using the default {@link TokenSettingsCodec}.
   *
   * @see #defaultSettingsCodec
   *
   * @param input stream from wich to construct the matrix
   * @param minimumShingleSize minimum number of tokens in any shingle.
   * @param maximumShingleSize maximum number of tokens in any shingle.
   * @param spacerCharacter character to use between texts of the token parts in a shingle. null for none.
   * @param ignoringSinglePrefixOrSuffixShingle if true, shingles that only contains permutation of the first of the last column will not be produced as shingles. Useful when adding boundary marker tokens such as '^' and '$'.
   */
  public ShingleMatrixFilter(TokenStream input, int minimumShingleSize, int maximumShingleSize, Character spacerCharacter, boolean ignoringSinglePrefixOrSuffixShingle) {
    this(input, minimumShingleSize, maximumShingleSize, spacerCharacter, ignoringSinglePrefixOrSuffixShingle, defaultSettingsCodec);
  }


  /**
   * Creates a shingle filter with ad hoc parameter settings.
   *
   * @param input stream from wich to construct the matrix
   * @param minimumShingleSize minimum number of tokens in any shingle.
   * @param maximumShingleSize maximum number of tokens in any shingle.
   * @param spacerCharacter character to use between texts of the token parts in a shingle. null for none.
   * @param ignoringSinglePrefixOrSuffixShingle if true, shingles that only contains permutation of the first of the last column will not be produced as shingles. Useful when adding boundary marker tokens such as '^' and '$'.
   * @param settingsCodec codec used to read input token weight and matrix positioning.
   */
  public ShingleMatrixFilter(TokenStream input, int minimumShingleSize, int maximumShingleSize, Character spacerCharacter, boolean ignoringSinglePrefixOrSuffixShingle, TokenSettingsCodec settingsCodec) {
    this.input = input;
    this.minimumShingleSize = minimumShingleSize;
    this.maximumShingleSize = maximumShingleSize;
    this.spacerCharacter = spacerCharacter;
    this.ignoringSinglePrefixOrSuffixShingle = ignoringSinglePrefixOrSuffixShingle;
    this.settingsCodec = settingsCodec;
  }

  // internal filter instance variables

  /** iterator over the current matrix row permutations */
  private Iterator permutations;

  /** the current permutation of tokens used to produce shingles */
  private List currentPermuationTokens;
  /** index to what row a token in currentShingleTokens represents*/
  private List currentPermutationRows;

  private int currentPermutationTokensStartOffset;
  private int currentShingleLength;

  /**
   * a set containing shingles that has been the result of a call to next(Token),
   * used to avoid producing the same shingle more than once.
   */
  private Set shinglesSeen = new HashSet();


  public void reset() throws IOException {
    permutations = null;
    shinglesSeen.clear();
    input.reset();
  }

  private Matrix matrix;

  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (matrix == null) {
      matrix = new Matrix();
      // fill matrix with maximumShingleSize columns
      while (matrix.columns.size() < maximumShingleSize && readColumn()) {
        // this loop looks ugly
      }
    }

    if (currentPermuationTokens != null) {
      currentShingleLength++;

      if (currentShingleLength + currentPermutationTokensStartOffset <= currentPermuationTokens.size()
          && currentShingleLength <= maximumShingleSize) {

        // it is possible to create at least one more shingle of the current matrix permutation

        if (ignoringSinglePrefixOrSuffixShingle
            && currentShingleLength == 1
            && (((Matrix.Column.Row) currentPermutationRows.get(currentPermutationTokensStartOffset)).getColumn().isFirst() || ((Matrix.Column.Row) currentPermutationRows.get(currentPermutationTokensStartOffset)).getColumn().isLast())) {
          return next(reusableToken);
        }

        int termLength = 0;

        List shingle = new ArrayList();

        for (int i = 0; i < currentShingleLength; i++) {
          Token shingleToken = (Token) currentPermuationTokens.get(i + currentPermutationTokensStartOffset);
          termLength += shingleToken.termLength();
          shingle.add(shingleToken);
        }
        if (spacerCharacter != null) {
          termLength += currentShingleLength - 1;
        }

        // only produce shingles that not already has been created
        if (!shinglesSeen.add(shingle)) {
          return next(reusableToken);
        }

        // shingle token factory
        StringBuffer sb = new StringBuffer(termLength + 10); // paranormal ability to foresee the future.
        for (Iterator iterator = shingle.iterator(); iterator.hasNext();) {
          Token shingleToken = (Token) iterator.next();
          if (spacerCharacter != null && sb.length() > 0) {
            sb.append(spacerCharacter);
          }
          sb.append(shingleToken.termBuffer(), 0, shingleToken.termLength());
        }
        reusableToken.setTermBuffer(sb.toString());
        updateToken(reusableToken, shingle, currentPermutationTokensStartOffset, currentPermutationRows, currentPermuationTokens);

        return reusableToken;

      } else {

        // it is NOT possible to create one more shingles of the current matrix permutation

        if (currentPermutationTokensStartOffset < currentPermuationTokens.size() - 1) {
          // reset shingle size and move one step to the right in the current tokens permutation
          currentPermutationTokensStartOffset++;
          currentShingleLength = minimumShingleSize - 1;
          return next(reusableToken);
        }


        if (permutations == null) {
          // todo does this ever occur?
          return null;
        }


        if (!permutations.hasNext()) {

          // load more data (if available) to the matrix

          if (input != null && readColumn()) {
            // don't really care, we just read it.
          }

          // get rith of resources

          // delete the first column in the matrix
          Matrix.Column deletedColumn = (Matrix.Column) matrix.columns.remove(0);

          // remove all shingles seen that include any of the tokens from the deleted column.
          List deletedColumnTokens = new ArrayList();
          for (Iterator iterator = deletedColumn.getRows().iterator(); iterator.hasNext();) {
            Matrix.Column.Row row = (Matrix.Column.Row) iterator.next();
            for (Iterator rowIter = row.getTokens().iterator(); rowIter.hasNext();) {
              Object o = rowIter.next();//Token
              deletedColumnTokens.add(o);
            }

          }
          for (Iterator shinglesSeenIterator = shinglesSeen.iterator(); shinglesSeenIterator.hasNext();) {
            List shingle = (List) shinglesSeenIterator.next();
            for (Iterator deletedIter = deletedColumnTokens.iterator(); deletedIter.hasNext();) {
              Token deletedColumnToken = (Token) deletedIter.next();
              if (shingle.contains(deletedColumnToken)) {
                shinglesSeenIterator.remove();
                break;
              }
            }
          }


          if (matrix.columns.size() < minimumShingleSize) {
            // exhausted
            return null;
          }

          // create permutations of the matrix it now looks
          permutations = matrix.permutationIterator();
        }

        nextTokensPermutation();
        return next(reusableToken);

      }
    }

    if (permutations == null) {
      permutations = matrix.permutationIterator();
    }

    if (!permutations.hasNext()) {
      return null;
    }

    nextTokensPermutation();

    return next(reusableToken);
  }

  /**
   * get next permutation of row combinations,
   * creates list of all tokens in the row and
   * an index from each such token to what row they exist in.
   * finally resets the current (next) shingle size and offset.
   */
  private void nextTokensPermutation() {
    Matrix.Column.Row[] rowsPermutation;
    rowsPermutation = (Matrix.Column.Row[]) permutations.next();
    List currentPermutationRows = new ArrayList();
    List currentPermuationTokens = new ArrayList();
    for (int i = 0; i < rowsPermutation.length; i++) {
      Matrix.Column.Row row = rowsPermutation[i];
      for (Iterator iterator = row.getTokens().iterator(); iterator.hasNext();) {
        currentPermuationTokens.add(iterator.next());
        currentPermutationRows.add(row);
      }
    }
    this.currentPermuationTokens = currentPermuationTokens;
    this.currentPermutationRows = currentPermutationRows;

    currentPermutationTokensStartOffset = 0;
    currentShingleLength = minimumShingleSize - 1;

  }

  /**
   * Final touch of a shingle token before it is passed on to the consumer from method {@link #next(org.apache.lucene.analysis.Token)}.
   *
   * Calculates and sets type, flags, position increment, start/end offsets and weight.
   *
   * @param token Shingle token
   * @param shingle Tokens used to produce the shingle token.
   * @param currentPermutationStartOffset Start offset in parameter currentPermutationTokens
   * @param currentPermutationRows index to Matrix.Column.Row from the position of tokens in parameter currentPermutationTokens
   * @param currentPermuationTokens tokens of the current permutation of rows in the matrix.
   */
  public void updateToken(Token token, List shingle, int currentPermutationStartOffset, List currentPermutationRows, List currentPermuationTokens) {
    token.setType(ShingleMatrixFilter.class.getName());
    token.setFlags(0);
    token.setPositionIncrement(1);
    token.setStartOffset(((Token) shingle.get(0)).startOffset());
    token.setEndOffset(((Token) shingle.get(shingle.size() - 1)).endOffset());
    settingsCodec.setWeight(token, calculateShingleWeight(token, shingle, currentPermutationStartOffset, currentPermutationRows, currentPermuationTokens));
  }

  /**
   * Evaluates the new shingle token weight.
   *
   * for (shingle part token in shingle)
   * weight +=  shingle part token weight * (1 / sqrt(all shingle part token weights summed))
   *
   * This algorithm gives a slightly greater score for longer shingles
   * and is rather penalising to great shingle token part weights.  
   *
   * @param shingleToken token returned to consumer
   * @param shingle tokens the tokens used to produce the shingle token.
   * @param currentPermutationStartOffset start offset in parameter currentPermutationRows and currentPermutationTokens.
   * @param currentPermutationRows an index to what matrix row a token in parameter currentPermutationTokens exist.
   * @param currentPermuationTokens all tokens in the current row permutation of the matrix. A sub list (parameter offset, parameter shingle.size) equals parameter shingle.
   * @return weight to be set for parameter shingleToken
   */
  public float calculateShingleWeight(Token shingleToken, List shingle, int currentPermutationStartOffset, List currentPermutationRows, List currentPermuationTokens) {
    double[] weights = new double[shingle.size()];

    double total = 0f;
    double top = 0d;


    for (int i=0; i<weights.length; i++) {
      weights[i] = settingsCodec.getWeight((Token) shingle.get(i));

      double tmp = weights[i];
      if (tmp > top) {
        top = tmp;
      }
      total += tmp;
    }

    double factor = 1d / Math.sqrt(total);

    double weight = 0d;
    for (int i = 0; i < weights.length; i++) {
      double partWeight = weights[i];
      weight += partWeight * factor;
    }

    return (float) weight;
  }


  private Token readColumnBuf;

  /**
   * Loads one column from the token stream.
   *
   * When the last token is read from the token stream it will column.setLast(true);
   *
   * @return true if it manage to read one more column from the input token stream
   * @throws IOException if the matrix source input stream throws an exception
   */
  private boolean readColumn() throws IOException {

    Token token;
    if (readColumnBuf != null) {
      token = readColumnBuf;
      readColumnBuf = null;
    } else {
      token = input.next(new Token());
    }

    if (token == null) {
      return false;
    }

    Matrix.Column currentReaderColumn = matrix.new Column();
    Matrix.Column.Row currentReaderRow = currentReaderColumn.new Row();

    currentReaderRow.getTokens().add(token);
    TokenPositioner tokenPositioner;
    while ((readColumnBuf = input.next(new Token())) != null
        && (tokenPositioner = settingsCodec.getTokenPositioner(readColumnBuf)) != TokenPositioner.newColumn) {

      if (tokenPositioner == TokenPositioner.sameRow) {
        currentReaderRow.getTokens().add(readColumnBuf);
      } else /*if (tokenPositioner == TokenPositioner.newRow)*/ {
        currentReaderRow = currentReaderColumn.new Row();
        currentReaderRow.getTokens().add(readColumnBuf);
      }
      readColumnBuf = null;

    }

    if (readColumnBuf == null) {
      readColumnBuf = input.next(new Token());
      if (readColumnBuf == null) {
        currentReaderColumn.setLast(true);
      }
    }


    return true;

  }


  /**
   * A column focused matrix in three dimensions:
   *
   * <pre>
   * Token[column][row][z-axis] {
   *     {{hello}, {greetings, and, salutations}},
   *     {{world}, {earth}, {tellus}}
   * };
   * </pre>
   *
   * todo consider row groups
   * to indicate that shingles is only to contain permutations with texts in that same row group.
   *
   */
  public static class Matrix {

    private boolean columnsHasBeenCreated = false;

    private List columns = new ArrayList();

    public List getColumns() {
      return columns;
    }

    public class Column {

      private boolean last;
      private boolean first;

      public Matrix getMatrix() {
        return Matrix.this;
      }

      public Column(Token token) {
        this();
        Row row = new Row();
        row.getTokens().add(token);
      }

      public Column() {
        synchronized (Matrix.this) {
          if (!columnsHasBeenCreated) {
            this.setFirst(true);
            columnsHasBeenCreated = true;
          }
        }
        Matrix.this.columns.add(this);
      }

      private List rows = new ArrayList();

      public List getRows() {
        return rows;
      }


      public int getIndex() {
        return Matrix.this.columns.indexOf(this);
      }

      public String toString() {
        return "Column{" +
            "first=" + first +
            ", last=" + last +
            ", rows=" + rows +
            '}';
      }

      public boolean isFirst() {
        return first;
      }

      public void setFirst(boolean first) {
        this.first = first;
      }

      public void setLast(boolean last) {
        this.last = last;
      }

      public boolean isLast() {
        return last;
      }

      public class Row {

        public Column getColumn() {
          return Column.this;
        }

        private List tokens = new LinkedList();

        public Row() {
          Column.this.rows.add(this);
        }

        public int getIndex() {
          return Column.this.rows.indexOf(this);
        }

        public List getTokens() {
          return tokens;
        }

        public void setTokens(List tokens) {
          this.tokens = tokens;
        }

//        public int getStartOffset() {
//          int ret = tokens[0].startOffset();
//          if (getIndex() > 0 && ret == 0) {
//            ret = Column.this.rows.get(0).getStartOffset();
//          }
//          return ret;
//        }
//
//        public int getEndOffset() {
//          int ret = tokens[tokens.length - 1].endOffset();
//          if (getIndex() > 0 && ret == 0) {
//            ret = Column.this.rows.get(0).getEndOffset();
//          }
//          return ret;
//        }

        public String toString() {
          return "Row{" +
              "index=" + getIndex() +
              ", tokens=" + (tokens == null ? null : tokens) +
              '}';
        }
      }

    }


    public Iterator permutationIterator() {

      return new Iterator() {

        private int[] columnRowCounters = new int[columns.size()];

        public void remove() {
          throw new IllegalStateException("not implemented");
        }

        public boolean hasNext() {
          int s = columnRowCounters.length;
          return s != 0 && columnRowCounters[s - 1] < ((Column) columns.get(s - 1)).getRows().size();
        }

        public Object next() {
          if (!hasNext()) {
            throw new NoSuchElementException("no more elements");
          }

          Column.Row[] rows = new Column.Row[columnRowCounters.length];

          for (int i = 0; i < columnRowCounters.length; i++) {
            rows[i] = (Matrix.Column.Row) ((Column) columns.get(i)).rows.get(columnRowCounters[i]);
          }
          incrementColumnRowCounters();

          return rows;
        }

        private void incrementColumnRowCounters() {
          for (int i = 0; i < columnRowCounters.length; i++) {
            columnRowCounters[i]++;
            if (columnRowCounters[i] == ((Column) columns.get(i)).rows.size() &&
                i < columnRowCounters.length - 1) {
              columnRowCounters[i] = 0;
            } else {
              break;
            }
          }
        }
      };
    }

    public String toString() {
      return "Matrix{" +
          "columns=" + columns +
          '}';
    }
  }


  public int getMinimumShingleSize() {
    return minimumShingleSize;
  }

  public void setMinimumShingleSize(int minimumShingleSize) {
    this.minimumShingleSize = minimumShingleSize;
  }

  public int getMaximumShingleSize() {
    return maximumShingleSize;
  }

  public void setMaximumShingleSize(int maximumShingleSize) {
    this.maximumShingleSize = maximumShingleSize;
  }


  public Matrix getMatrix() {
    return matrix;
  }

  public void setMatrix(Matrix matrix) {
    this.matrix = matrix;
  }

  public Character getSpacerCharacter() {
    return spacerCharacter;
  }

  public void setSpacerCharacter(Character spacerCharacter) {
    this.spacerCharacter = spacerCharacter;
  }

  public boolean isIgnoringSinglePrefixOrSuffixShingle() {
    return ignoringSinglePrefixOrSuffixShingle;
  }

  public void setIgnoringSinglePrefixOrSuffixShingle(boolean ignoringSinglePrefixOrSuffixShingle) {
    this.ignoringSinglePrefixOrSuffixShingle = ignoringSinglePrefixOrSuffixShingle;
  }

  /**
   * Using this codec makes a {@link ShingleMatrixFilter} act like {@link org.apache.lucene.analysis.shingle.ShingleFilter}.
   * It produces the most simple sort of shingles, ignoring token position increments, et c.
   *
   * It adds each token as a new column.
   */
  public static class OneDimensionalNonWeightedTokenSettingsCodec extends TokenSettingsCodec {

    public TokenPositioner getTokenPositioner(Token token) throws IOException {
      return TokenPositioner.newColumn;
    }

    public void setTokenPositioner(Token token, TokenPositioner tokenPositioner) {
    }

    public float getWeight(Token token) {
      return 1f;
    }

    public void setWeight(Token token, float weight) {
    }

  }


  /**
   * A codec that creates a two dimensional matrix
   * by treating tokens from the input stream with 0 position increment
   * as new rows to the current column.
   */
  public static class TwoDimensionalNonWeightedSynonymTokenSettingsCodec extends TokenSettingsCodec {

    public TokenPositioner getTokenPositioner(Token token) throws IOException {
      if (token.getPositionIncrement() == 0) {
        return TokenPositioner.newRow;
      } else {
        return TokenPositioner.newColumn;
      }
    }

    public void setTokenPositioner(Token token, TokenPositioner tokenPositioner) {
      throw new UnsupportedOperationException();
    }

    public float getWeight(Token token) {
      return 1f;
    }

    public void setWeight(Token token, float weight) {
    }

  }

  /**
   * A full featured codec not to be used for something serious.
   *
   * It takes complete control of
   * payload for weight
   * and the bit flags for positioning in the matrix.
   *
   * Mainly exist for demonstrational purposes.
   */
  public static class SimpleThreeDimensionalTokenSettingsCodec extends TokenSettingsCodec {

    /**
     * @param token
     * @return the token flags int value as TokenPosition
     * @throws IOException
     */
    public TokenPositioner getTokenPositioner(Token token) throws IOException {
      switch (token.getFlags()) {
        case 0:
          return TokenPositioner.newColumn;
        case 1:
          return TokenPositioner.newRow;
        case 2:
          return TokenPositioner.sameRow;
      }
      throw new IOException("Unknown matrix positioning of token " + token);
    }

    /**
     * Sets the TokenPositioner as token flags int value.
     *
     * @param token
     * @param tokenPositioner
     */
    public void setTokenPositioner(Token token, TokenPositioner tokenPositioner) {
      token.setFlags(tokenPositioner.getIndex());
    }

    /**
     * Returns a 32 bit float from the payload, or 1f it null.
     *
     * @param token
     * @return
     */
    public float getWeight(Token token) {
      if (token.getPayload() == null || token.getPayload().getData() == null) {
        return 1f;
      } else {
        return PayloadHelper.decodeFloat(token.getPayload().getData());
      }
    }

    /**
     * Stores a 32 bit float in the payload, or set it to null if 1f;
     * @param token
     * @param weight
     */
    public void setWeight(Token token, float weight) {
      if (weight == 1f) {
        token.setPayload(null);
      } else {
        token.setPayload(new Payload(PayloadHelper.encodeFloat(weight)));
      }
    }

  }


}
