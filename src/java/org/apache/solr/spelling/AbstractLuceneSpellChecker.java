package org.apache.solr.spelling;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;


/**
 * Abstract base class for all Lucene-based spell checking implementations.
 * 
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/SpellCheckComponent">SpellCheckComponent</a>
 * for more details.
 * </p>
 * 
 * @since solr 1.3
 */
public abstract class AbstractLuceneSpellChecker extends SolrSpellChecker {
  public static final Logger LOG = Logger.getLogger(AbstractLuceneSpellChecker.class.getName());
  
  public static final String SPELLCHECKER_ARG_NAME = "spellchecker";
  public static final String LOCATION = "sourceLocation";
  public static final String INDEX_DIR = "spellcheckIndexDir";
  public static final String ACCURACY = "accuracy";
  public static final String STRING_DISTANCE = "distanceMeasure";
  public static final String FIELD_TYPE = "fieldType";
  protected String field;
  protected String fieldTypeName;
  protected org.apache.lucene.search.spell.SpellChecker spellChecker;

  protected String sourceLocation;
  /*
  * The Directory containing the Spell checking index
  * */
  protected Directory index;
  protected Dictionary dictionary;

  public static final int DEFAULT_SUGGESTION_COUNT = 5;
  protected String indexDir;
  protected float accuracy = 0.5f;
  public static final String FIELD = "field";

  public String init(NamedList config, SolrCore core) {
    super.init(config, core);
    indexDir = (String) config.get(INDEX_DIR);
    String accuracy = (String) config.get(ACCURACY);
    //If indexDir is relative then create index inside core.getDataDir()
    if (indexDir != null)   {
      if (!new File(indexDir).isAbsolute()) {
        indexDir = core.getDataDir() + File.separator + indexDir;
      }
    }
    sourceLocation = (String) config.get(LOCATION);
    field = (String) config.get(FIELD);
    String strDistanceName = (String)config.get(STRING_DISTANCE);
    StringDistance sd = null;
    if (strDistanceName != null) {
      sd = (StringDistance) core.getResourceLoader().newInstance(strDistanceName);
      //TODO: Figure out how to configure options.  Where's Spring when you need it?  Or at least BeanUtils...
    } else {
      sd = new LevensteinDistance();
    }
    try {
      initIndex();
      spellChecker = new SpellChecker(index, sd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (accuracy != null) {
      try {
        this.accuracy = Float.parseFloat(accuracy);
        spellChecker.setAccuracy(this.accuracy);
      } catch (NumberFormatException e) {
        throw new RuntimeException(
                "Unparseable accuracy given for dictionary: " + name, e);
      }
    }
    if (field != null && core.getSchema().getFieldTypeNoEx(field) != null)  {
      analyzer = core.getSchema().getFieldType(field).getQueryAnalyzer();
    }
    fieldTypeName = (String) config.get(FIELD_TYPE);
    if (core.getSchema().getFieldTypes().containsKey(fieldTypeName))  {
      FieldType fieldType = core.getSchema().getFieldTypes().get(fieldTypeName);
      analyzer = fieldType.getQueryAnalyzer();
    }
    if (analyzer == null)   {
      LOG.info("Using WhitespaceAnalzyer for dictionary: " + name);
      analyzer = new WhitespaceAnalyzer();
    }
    return name;
  }
  
  @SuppressWarnings("unchecked")
  public SpellingResult getSuggestions(Collection<Token> tokens,
                                       IndexReader reader, int count, boolean onlyMorePopular,
                                       boolean extendedResults)
          throws IOException {
    SpellingResult result = new SpellingResult(tokens);
    reader = determineReader(reader);
    Term term = field != null ? new Term(field, "") : null;
    for (Token token : tokens) {
      String tokenText = new String(token.termBuffer(), 0, token.termLength());
      String[] suggestions = spellChecker.suggestSimilar(tokenText, (int) Math.max(count, AbstractLuceneSpellChecker.DEFAULT_SUGGESTION_COUNT),
            field != null ? reader : null, //workaround LUCENE-1295
            field,
            onlyMorePopular);
      if (suggestions.length == 1 && suggestions[0].equals(tokenText)) {
        //These are spelled the same, continue on
        continue;
      }

      if (extendedResults == true && reader != null && field != null) {
        term = term.createTerm(tokenText);
        result.add(token, reader.docFreq(term));
        int countLimit = Math.min(count, suggestions.length);
        for (int i = 0; i < countLimit; i++) {
          term = term.createTerm(suggestions[i]);
          result.add(token, suggestions[i], reader.docFreq(term));
        }
      } else {
        if (suggestions.length > 0) {
          List<String> suggList = Arrays.asList(suggestions);
          if (suggestions.length > count) {
            suggList = suggList.subList(0, count);
          }
          result.add(token, suggList);
        }
      }
    }
    return result;
  }

  protected IndexReader determineReader(IndexReader reader) {
    return reader;
  }

  public void reload() throws IOException {
    spellChecker.setSpellIndex(index);

  }

  /**
   * Initialize the {@link #index} variable based on the {@link #indexDir}.  Does not actually create the spelling index.
   *
   * @throws IOException
   */
  protected void initIndex() throws IOException {
    if (indexDir != null) {
      index = FSDirectory.getDirectory(indexDir);
    } else {
      index = new RAMDirectory();
    }
  }

  /*
  * @return the Accuracy used for the Spellchecker
  * */
  public float getAccuracy() {
    return accuracy;
  }

  /*
  * @return the Field used
  *
  * */
  public String getField() {
    return field;
  }

  /*
  *
  * @return the FieldType name.
  * */
  public String getFieldTypeName() {
    return fieldTypeName;
  }


  /*
  * @return the Index directory
  * */
  public String getIndexDir() {
    return indexDir;
  }

  /*
  * @return the location of the source
  * */
  public String getSourceLocation() {
    return sourceLocation;
  }
}
