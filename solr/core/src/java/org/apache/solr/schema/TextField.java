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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.parser.SolrQueryParserBase;
import org.apache.solr.query.SolrRangeQuery;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/** <code>TextField</code> is the basic type for configurable text analysis.
 * Analyzers for field types using this implementation should be defined in the schema.
 *
 */
public class TextField extends FieldType implements SchemaAware {

  protected static final String DV_SORT_SUFFIX_ARGNAME = "dvSortSuffix";
  protected static final String DV_SORT_TYPE_ARGNAME = "dvSortType";
  private static final String DV_SORT_PURPOSE_SUFFIX = "_dvSort";

  protected static final String MAX_CHARS_FOR_DOC_VALUES_ARGNAME = "maxCharsForDocValues";
  public static final int DEFAULT_MAX_CHARS_FOR_DOC_VALUES = 1024;

  private boolean explicitMaxCharsForDocValues = false;
  private int maxCharsForDocValues = DEFAULT_MAX_CHARS_FOR_DOC_VALUES;
  private PolyFieldHelper sortSubfieldHelper;

  protected static final String MAX_DOC_VALUES_BYTES_ARGNAME = "maxDocValuesBytes";
  protected static final String ANALYZED_DOC_VALUES_ARGNAME = "analyzedDocValues";
  protected static final String DOC_VALUES_PURPOSE_ARGNAME = "dvPurpose";

  protected static final String DV_STORED_SUFFIX_ARGNAME = "dvStoredSuffix";
  protected static final String DV_STORED_TYPE_ARGNAME = "dvStoredType";
  private static final String DV_STORED_PURPOSE_SUFFIX = "_dvStored";

  public static final int DEFAULT_MAX_DOC_VALUES_BYTES = 32766;

  public static enum DocValuesPurpose {
    POLY, // the default -- a polyField with different subfields for each purpose
    SORT, // sort dv inline for this field
    VALUE_ACCESS, // stored-type dv inline for this field
    FACET // analyzed inline dvs for tokens
  }

  //nocommit: default to true for testing. Ultimately the default may want to be version-dependent,
  //nocommit: for backward compatibility of SortableTextField docValues ...
  public static final boolean DEFAULT_ANALYZED_DOC_VALUES = true;

  private boolean explicitMaxDocValuesBytes = false;
  protected int maxDocValuesBytes = DEFAULT_MAX_DOC_VALUES_BYTES;
  protected boolean analyzedDocValues = DEFAULT_ANALYZED_DOC_VALUES;

  protected static final Map<String, String> DV_DEFAULT_SUBFIELD_PROPERTIES;

  static {
    Map<String, String> props = new HashMap<>(6);
    props.put("indexed", "false");
    props.put("stored", "false");
    props.put("docValues", "true");
    DV_DEFAULT_SUBFIELD_PROPERTIES = Collections.unmodifiableMap(props);
  }

  /*
   * Generally polyField==true for "user-facing" fields, where this fieldType instance will mediate
   * access to various docValues representations for different use cases. If set to false, all docValues
   * access resolves to analyzed docValues for this field per se.
   */
  protected DocValuesPurpose dvPurpose = DocValuesPurpose.POLY;
  private PolyFieldHelper storedSubfieldHelper;

  protected boolean autoGeneratePhraseQueries;
  protected boolean enableGraphQueries;
  protected SolrQueryParserBase.SynonymQueryStyle synonymQueryStyle;

  /**
   * Analyzer set by schema for text types to use when searching fields
   * of this type, subclasses can set analyzer themselves or override
   * getIndexAnalyzer()
   * This analyzer is used to process wildcard, prefix, regex and other multiterm queries. It
   * assembles a list of tokenizer +filters that "make sense" for this, primarily accent folding and
   * lowercasing filters, and charfilters.
   *
   * @see #getMultiTermAnalyzer
   * @see #setMultiTermAnalyzer
   */
  protected Analyzer multiTermAnalyzer=null;
  private boolean isExplicitMultiTermAnalyzer = false;

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    properties |= TOKENIZED;
    if (schema.getVersion() > 1.1F &&
        // only override if it's not explicitly true
        0 == (trueProperties & OMIT_TF_POSITIONS)) {
      properties &= ~OMIT_TF_POSITIONS;
    }
    if (schema.getVersion() > 1.3F) {
      autoGeneratePhraseQueries = false;
    } else {
      autoGeneratePhraseQueries = true;
    }
    String autoGeneratePhraseQueriesStr = args.remove(AUTO_GENERATE_PHRASE_QUERIES);
    if (autoGeneratePhraseQueriesStr != null)
      autoGeneratePhraseQueries = Boolean.parseBoolean(autoGeneratePhraseQueriesStr);

    synonymQueryStyle = SolrQueryParserBase.SynonymQueryStyle.AS_SAME_TERM;
    String synonymQueryStyle = args.remove(SYNONYM_QUERY_STYLE);
    if (synonymQueryStyle != null) {
      this.synonymQueryStyle = SolrQueryParserBase.SynonymQueryStyle.valueOf(synonymQueryStyle.toUpperCase(Locale.ROOT));
    }
    
    enableGraphQueries = true;
    String enableGraphQueriesStr = args.remove(ENABLE_GRAPH_QUERIES);
    if (enableGraphQueriesStr != null)
      enableGraphQueries = Boolean.parseBoolean(enableGraphQueriesStr);

    initGeneralDocValuesArgs(args);

    initSortDocValuesArgs(schema, args);

    initStoredDocValuesArgs(schema, args);

    super.init(schema, args);
  }

  private void initGeneralDocValuesArgs(Map<String, String> args) {
    String analyzedDocValuesStr = args.remove(ANALYZED_DOC_VALUES_ARGNAME);
    if (analyzedDocValuesStr != null) {
      analyzedDocValues = Boolean.parseBoolean(analyzedDocValuesStr);
    }

    String dvPurposeStr = args.remove(DOC_VALUES_PURPOSE_ARGNAME);
    if (dvPurposeStr != null) {
      dvPurpose = DocValuesPurpose.valueOf(dvPurposeStr.toUpperCase(Locale.ENGLISH));
      switch (dvPurpose) {
        case VALUE_ACCESS:
        case SORT:
          // not a polyfield, so we're using *this* field for VALUE_ACCESS or SORT docValues
          if (analyzedDocValuesStr != null && analyzedDocValues) {
            throw new IllegalArgumentException(ANALYZED_DOC_VALUES_ARGNAME+" cannot be set to true for purpose "+dvPurpose+" ("+getTypeName()+")");
          }
          analyzedDocValues = false;
          break;
        case FACET:
          // explicit facet purpose directly on this field implies analyzedDocValues=true
          if (analyzedDocValuesStr != null && analyzedDocValues) {
            throw new IllegalArgumentException(ANALYZED_DOC_VALUES_ARGNAME+" cannot be set to false for purpose "+dvPurpose+" ("+getTypeName()+")");
          }
          analyzedDocValues = true;
          break;
        case POLY:
          // just proceed
      }
      // explicit dvPurpose implies a default to docValues=true
      if (!on(falseProperties, DOC_VALUES)) {
        properties |= DOC_VALUES;
      }
    }
  }

  private void initSortDocValuesArgs(IndexSchema schema, Map<String, String> args) {
    final String maxS = args.remove(MAX_CHARS_FOR_DOC_VALUES_ARGNAME);
    if (maxS != null) {
      explicitMaxCharsForDocValues = true;
      maxCharsForDocValues = Integer.parseInt(maxS);
      if (maxCharsForDocValues < 0) {
        // nocommit: this slightly changes logic from SortableTextField, which considered "0" to indicate no limit
        maxCharsForDocValues = Integer.MAX_VALUE;
      }
    }
    if (maxCharsForDocValues == 0) {
      // effectively disables dv for sort; nocommit: no test coverage for this yet.
      return;
    }
    sortSubfieldHelper = new PolyFieldHelper(this, DV_SORT_SUFFIX_ARGNAME, DV_SORT_TYPE_ARGNAME, DV_SORT_PURPOSE_SUFFIX,
        DV_DEFAULT_SUBFIELD_PROPERTIES, Collections.emptyMap(),
        MULTIVALUED | USE_DOCVALUES_AS_STORED | DOC_VALUES | SORT_MISSING_FIRST | SORT_MISSING_LAST);
    sortSubfieldHelper.init(schema, args);

    if (sortSubfieldHelper.hasExplicitType()) {
      // implies docValues=true, unless user has explicitly requested docValues=false
      if (!on(falseProperties, DOC_VALUES)) {
        properties |= DOC_VALUES;
      }
      // defer checking validity of explicitMaxCharsForDocValues until inform(...)
    }
  }

  private void initStoredDocValuesArgs(IndexSchema schema, Map<String, String> args) {
    String maxDocValuesBytesStr = args.remove(MAX_DOC_VALUES_BYTES_ARGNAME);
    if (maxDocValuesBytesStr != null) {
      explicitMaxDocValuesBytes = true;
      maxDocValuesBytes = Integer.parseInt(maxDocValuesBytesStr);
      if (maxDocValuesBytes < 0) {
        maxDocValuesBytes = Integer.MAX_VALUE;
      }
    }

    storedSubfieldHelper = new PolyFieldHelper(this, DV_STORED_SUFFIX_ARGNAME, DV_STORED_TYPE_ARGNAME, DV_STORED_PURPOSE_SUFFIX,
        DV_DEFAULT_SUBFIELD_PROPERTIES, Collections.singletonMap(MAX_DOC_VALUES_BYTES_ARGNAME, Integer.toString(maxDocValuesBytes)),
        MULTIVALUED | DOC_VALUES | USE_DOCVALUES_AS_STORED);

    storedSubfieldHelper.init(schema, args);

    boolean impliedUdvas = false;
    if (storedSubfieldHelper.hasExplicitType()) {
      impliedUdvas = true;
      if (explicitMaxDocValuesBytes) {
        throw new IllegalArgumentException(MAX_DOC_VALUES_BYTES_ARGNAME + " should be set on the delegate "
            + storedSubfieldHelper + ", not directly on fieldType=\""+getTypeName()+"\"");
      }
    } else if (explicitMaxDocValuesBytes) {
      impliedUdvas = true;
    } else if (!on(trueProperties, USE_DOCVALUES_AS_STORED)) {
      // for TextField, stored docValues are potentially quite costly, so we only want to create these
      // if it's explicitly configured
      properties &= ~USE_DOCVALUES_AS_STORED;
    }

    if (impliedUdvas && !on(falseProperties, DOC_VALUES) && !on(falseProperties, USE_DOCVALUES_AS_STORED)) {
      // unless explicitly turned off, dvStored* or maxDocValuesBytes args imply DOC_VALUES and USE_DOC_VALUES_AS_STORED
      properties |= (DOC_VALUES | USE_DOCVALUES_AS_STORED);
    }
  }

  @Override
  public void inform(IndexSchema schema) {
    if (!isPolyField()) {
      return;
    }
    storedSubfieldHelper.inform(schema);

    sortSubfieldHelper.inform(schema);
    FieldType sortSubtype = sortSubfieldHelper.getExplicitType();
    if (sortSubtype != null && sortSubtype.getAnalyzedDocValuesType() != DocValuesType.NONE) {
      // subtype is analyzed, so it can (and we can't) filter for maxCharsForDocValues.
      if (explicitMaxCharsForDocValues) {
        throw new IllegalArgumentException("for type "+getTypeName()+", "+MAX_CHARS_FOR_DOC_VALUES_ARGNAME+" should be delegated to "
            + "explicit analyzed subfield "+sortSubfieldHelper);
      } else {
        // remove the default locally-enforced limit
        maxCharsForDocValues = Integer.MAX_VALUE;
      }
    }
  }

  @Override
  public boolean isPolyField() {
    return dvPurpose == DocValuesPurpose.POLY;
  }

  /**
   * {@inheritDoc}
   * this field type supports DocValues, this method is always a No-Op
   */
  @Override
  protected void checkSupportsDocValues() {
    // No-Op
  }

  @Override
  public DocValuesType getAnalyzedDocValuesType() {
    return analyzedDocValues ? DocValuesType.SORTED_SET : DocValuesType.NONE;
  }

  /**
   * Returns the Analyzer to be used when searching fields of this type when mult-term queries are specified.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getIndexAnalyzer
   */
  public Analyzer getMultiTermAnalyzer() {
    return multiTermAnalyzer;
  }

  public void setMultiTermAnalyzer(Analyzer analyzer) {
    this.multiTermAnalyzer = analyzer;
  }

  public boolean getAutoGeneratePhraseQueries() {
    return autoGeneratePhraseQueries;
  }
  
  public boolean getEnableGraphQueries() {
    return enableGraphQueries;
  }

  public SolrQueryParserBase.SynonymQueryStyle getSynonymQueryStyle() {return synonymQueryStyle;}

  /**
   * For whitebox tests
   */
  SchemaField getSortSchemaField(SchemaField parent) {
    return isPolyField() ? getSortDocValuesField(parent) : parent;
  }

  private boolean supportsDocValuesSort(SchemaField field) {
    return field.hasDocValues() && maxCharsForDocValues != 0;
  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    if (supportsDocValuesSort(field)) {
      // NOTE: sort wrt full field values, not fall through to historic token Uninversion behavior of TextField.
      return getStringSort(isPolyField() ? getSortDocValuesField(field) : field, reverse);
    }
    /* :TODO: maybe warn if isTokenized(), but doesn't use LimitTokenCountFilter in its chain? */
    return getSortedSetSortField(field,
                                 // historical behavior based on how the early versions of the FieldCache
                                 // would deal with multiple indexed terms in a singled valued field...
                                 //
                                 // Always use the 'min' value from the (Uninverted) "psuedo doc values"
                                 SortedSetSelector.Type.MIN,
                                 reverse, SortField.STRING_FIRST, SortField.STRING_LAST);
  }
  
  @Override
  public MultiValueSelector getDefaultMultiValueSelectorForSort(SchemaField field, boolean reverse) {
    if (!supportsDocValuesSort(field) && field.multiValued()) {
      // this preserves existing TextField behavior, where getSortField (above) falls through to
      // the uninverted behavior, attempt to sort on multiValued TextField is expected to fail
      // (see: BasicFunctionalityTest.testAbuseOfSort()) upon null return from this getDefaultMultiValueSelectorForSort
      // (see: SchemaField.checkSortability()).
      //
      // NOTE: if this restriction is removed, SortableTextField would no longer need to override this method
      //
      // nocommit: why is this restriction here? Could it just be removed? uninversion is over tokenized vals
      // nocommit: anyway, so isn't "multiValued" kind of arbitrary here anyway?
      return null;
    }
    return reverse ? MultiValueSelector.MAX : MultiValueSelector.MIN;
  }

  @Override
  public ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    // trivial base case
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, parser);
    }

    SortedSetSelector.Type selectorType = choice.getSortedSetSelectorType();
    if (null == selectorType) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              choice.toString() + " is not a supported option for picking a single value"
                              + " from the multivalued field: " + field.getName() +
                              " (type: " + this.getTypeName() + ")");
    }

    if (field.hasDocValues() && field.useDocValuesAsStored()) {
      return new StringRefValueSource(field, selectorType);
    } else {
      return getUninvertedValueSource(field.getName(), selectorType);
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    if (field.hasDocValues() && field.useDocValuesAsStored()) {
      return new StringRefValueSource(field);
    } else {
      return getUninvertedValueSource(field.getName(), SortedSetSelector.Type.MIN);
    }
  }
  
  private ValueSource getUninvertedValueSource(String fieldName, SortedSetSelector.Type selectorType) {
    // NOTE: it only really makes sense to do this by uninverting if the indexed
    // values use KeywordTokenizer ... e.g. if TextField is being used mainly for
    // the ability to run analyzers for normalization of what are otherwise essentially
    // not tokenized fields.
    // nocommit: should we just error out instead?
    return new SortedSetFieldSource(fieldName, selectorType);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return Type.SORTED_SET_BINARY;
  }

  /**
   * The base implementation of TextField docValues (for "stored"-type access) delegates
   * to a component field, so does not actually write docValues directly to "this" field.
   * In the event that this field is neither "indexed" nor "stored", this can mean that
   * dynamic fields would not be materialized in the Lucene index, and thus would not have
   * FieldInfos available to drive decisions based on the state of the on-disk index.
   *
   * Subclasses that do write docValues directly to "this" field must override this method
   * to return true (preventing the base implementation from writing a dummy BinaryDocValues
   * field for field visibility in the index).
   *
   * @param field - the field definition to evaluate
   * @return - true if this field is guaranteed to be materialized in the index, otherwise false
   */
  protected boolean materializesField(SchemaField field) {
    return field.indexed() || field.stored();
  }

  private boolean canReplaceStored(SchemaField field) {
    return (!field.multiValued() || maxCharsForDocValues == Integer.MAX_VALUE) && maxDocValuesBytes <= DEFAULT_MAX_DOC_VALUES_BYTES;
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    final List<IndexableField> fields = new ArrayList<>(4);
    IndexableField f = createField( field, value);
    if (f != null) {
      fields.add(f);
    }
    if (!field.hasDocValues()) {
      return fields;
    } else {
      switch (dvPurpose) {
        default:
          return fields;
        case SORT:
          boolean truncate = addSortFields(field, fields, value);
          if (truncate && field.useDocValuesAsStored()) {
            // if the user has explicitly configured useDocValuesAsStored, we need a special
            // check to fail docs where the values are too long -- we don't want to silently
            // accept and then have search queries returning partial values
            throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
               "Can not use field " + field.getName() + " with values longer then maxCharsForDocValues=" +
               maxCharsForDocValues + " when useDocValuesAsStored=true (length=" + value.toString().length() + ")");
          }
          return fields;
        case VALUE_ACCESS:
          fields.add(StrField.getDocValuesField(field, maxDocValuesBytes, value));
          return fields;
        case POLY:
          addValueAccessFields(field, fields, value);
          if (maxCharsForDocValues > 0) {
            addSortFields(field, fields, value);
          }
          return fields;
      }
    }
  }

  private void addValueAccessFields(SchemaField field, List<IndexableField> fields, Object value) {
    if (field.useDocValuesAsStored()) {
      if (fields.isEmpty() && !materializesField(field)) {
        // ensure that "this" field is represented in the index before delegating "actual"
        // field creation to subfields.
        fields.add(new BinaryDocValuesField(field.getName(), EMPTY_BYTES_REF));
      }
      SchemaField storedDVField = getStoredDocValuesField(field);
      fields.addAll(storedDVField.createFields(value));
    }
  }

  private boolean addSortFields(SchemaField field, List<IndexableField> fields, Object value) {
    if (value instanceof ByteArrayUtf8CharSequence) {
      ByteArrayUtf8CharSequence utf8 = (ByteArrayUtf8CharSequence) value;
      if (utf8.size() < maxCharsForDocValues) {
        addSortFields(field, fields, utf8, canReplaceStored(field));
        return false;
      }
    }
    final String origString = value.toString();
    final int origLegth = origString.length();
    final boolean truncate = maxCharsForDocValues < origLegth;
    final ByteArrayUtf8CharSequence utf8 = new ByteArrayUtf8CharSequence(truncate ? origString.substring(0, maxCharsForDocValues) : origString);

    addSortFields(field, fields, utf8, !truncate && canReplaceStored(field));
    return truncate;
  }

  private void addSortFields(SchemaField field, List<IndexableField> fields, ByteArrayUtf8CharSequence utf8, boolean canReplaceStored) {
    List<IndexableField> sortFields;
    switch (dvPurpose) {
      case SORT:
        BytesRef bytes = new BytesRef(utf8.getBuf(), utf8.offset(), utf8.size());
        if (field.multiValued()) {
          fields.add(new SortedSetDocValuesField(field.getName(), bytes));
        } else {
          fields.add(new SortedDocValuesField(field.getName(), bytes));
        }
        return;
      case POLY:
        SchemaField sortSubfield = getSortDocValuesField(field);
        sortFields = sortSubfield.createFields(utf8);
        break;
      default:
        throw new AssertionError("this should have been caught in init(...)");
    }

    if (canReplaceStored) {
      // nocommit: need better test coverage for this, e.g. in TestSortableTextField.testWhiteboxCreateFields()
      // we can use this "Sortable" dv as stored; remove superfluous "stored" dv field added by super.createFields()
      final Iterator<IndexableField> iter = fields.iterator();
      while (iter.hasNext()) {
        final IndexableField next = iter.next();
        final IndexableFieldType nextType = next.fieldType();
        if (nextType == SORTED || nextType == SORTED_SET) {
          assert getStoredDocValuesField(field).getName().equals(next.name());
          iter.remove();
          break;
        }
      }
    }
    fields.addAll(sortFields);
  }

  private static final IndexableFieldType SORTED = SortedDocValuesField.TYPE;
  private static final IndexableFieldType SORTED_SET = SortedSetDocValuesField.TYPE;

  protected final SchemaField getStoredDocValuesField(SchemaField field) {
    return storedSubfieldHelper.getSubfield(field);
  }

  protected final SchemaField getSortDocValuesField(SchemaField field) {
    return sortSubfieldHelper.getSubfield(field);
  }

  private static final BytesRef EMPTY_BYTES_REF = new BytesRef(BytesRef.EMPTY_BYTES);

  @Override
  public DocValuesRefIterator getDocValuesRefIterator(LeafReader reader, SchemaField schemaField) throws IOException {
    assert schemaField.hasDocValues();
    if (!isPolyField()) {
      if (schemaField.multiValued()) {
        return new SortedSetDocValuesRefIterator(DocValues.getSortedSet(reader, schemaField.getName()));
      } else {
        //SortedDocValues is a subclass of BinaryDocValues
        return new BinaryDocValuesRefIterator(DocValues.getSorted(reader, schemaField.getName()));
      }
    }
    SchemaField storedDVField = getStoredDocValuesField(schemaField);
    DocValuesRefIterator textFieldIter = storedDVField.getType().getDocValuesRefIterator(reader, storedDVField);
    if (!canReplaceStored(schemaField) || maxCharsForDocValues == 0) {
      return textFieldIter;
    }
    SchemaField sortSubfield = getSortDocValuesField(schemaField);
    DocValuesRefIterator sortDVRefIter = sortSubfield.getType().getDocValuesRefIterator(reader, sortSubfield);
    // nocommit: there are no tests for this yet!
    // if separate value exists for "stored" text field, return that; otherwise return the "sortable" value.
    return new UnionDocValuesRefIterator(new DocValuesRefIterator[] {textFieldIter, sortDVRefIter});
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), true);
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return parseFieldQuery(parser, getQueryAnalyzer(), field.getName(), externalVal);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.utf8ToString();
  }

  @Override
  protected boolean supportsAnalyzers() {
    return true;
  }

  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    Analyzer multiAnalyzer = getMultiTermAnalyzer();
    BytesRef lower = analyzeMultiTerm(field.getName(), part1, multiAnalyzer);
    BytesRef upper = analyzeMultiTerm(field.getName(), part2, multiAnalyzer);
    return new SolrRangeQuery(field.getName(), lower, upper, minInclusive, maxInclusive);
  }

  /**
   * Analyzes a text part using the provided {@link Analyzer} for a multi-term query.
   * <p>
   * Expects a single token to be used as multi-term term. This single token might also be filtered out
   * so zero token is supported and null is returned in this case.
   *
   * @return The multi-term term bytes; or null if there is no multi-term terms.
   * @throws SolrException If the {@link Analyzer} tokenizes more than one token;
   * or if an underlying {@link IOException} occurs.
   */
  public static BytesRef analyzeMultiTerm(String field, String part, Analyzer analyzerIn) {
    if (part == null || analyzerIn == null) return null;

    try (TokenStream source = analyzerIn.tokenStream(field, part)){
      source.reset();

      TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);

      if (!source.incrementToken()) {
        // Accept no tokens because it may have been filtered out by a StopFilter for example.
        return null;
      }
      BytesRef bytes = BytesRef.deepCopyOf(termAtt.getBytesRef());
      if (source.incrementToken())
        throw  new SolrException(SolrException.ErrorCode.BAD_REQUEST,"analyzer returned too many terms for multiTerm term: " + part);

      source.end();
      return bytes;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"error analyzing range part: " + part, e);
    }
  }


  static Query parseFieldQuery(QParser parser, Analyzer analyzer, String field, String queryText) {
    // note, this method always worked this way (but nothing calls it?) because it has no idea of quotes...
    return new QueryBuilder(analyzer).createPhraseQuery(field, queryText);
  }

  public void setIsExplicitMultiTermAnalyzer(boolean isExplicitMultiTermAnalyzer) {
    this.isExplicitMultiTermAnalyzer = isExplicitMultiTermAnalyzer;
  }

  public boolean isExplicitMultiTermAnalyzer() {
    return isExplicitMultiTermAnalyzer;
  }

  @Override
  public Object marshalSortValue(Object value) {
    return marshalStringSortValue(value);
  }

  @Override
  public Object unmarshalSortValue(Object value) {
    return unmarshalStringSortValue(value);
  }

  @Override
  public boolean isUtf8Field() {
    return true;
  }
}
