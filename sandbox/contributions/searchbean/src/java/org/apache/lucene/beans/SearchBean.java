/*
 * SearchBean.java
 *
 * Created on November 1, 2001, 10:31 AM
 */

package org.apache.lucene.beans;

import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;

import java.util.Vector;

import org.apache.lucene.beans.HitsIterator;

import java.io.IOException;

//import org.apache.log4j.Logger;

/**
 *
 * @author  peter carlson
 * @version 1.0
 */
public class SearchBean extends Object {

    static final String SORT_FIELD_RELEVANCE = "relevance";
    private String queryString = "";
    private String querySortField = SORT_FIELD_RELEVANCE; // default
    private String queryType = "";
    private Directory directory;
    private HitsIterator hitsIterator = null;
    private String defaultSearchField = "text";
    private long searchTime = 0;
    private Searcher searcher = null;
    // static Logger logger = Logger.getLogger(SearchBean.class.getName());
    // static Logger searchLogger = Logger.getLogger("searchLog");

    private SearchBean(){
    }

    /** Creates new SearchBean
     * @param path to index
     */
    public SearchBean(Directory directory) {
        this.directory = directory;
    }

    /** Creates new SearchBean
     * @param directory index
     * @param queryString string to search with
     */
    public SearchBean(Directory directory, String queryString) {
        this(directory);
        setQueryString(queryString);
    }

    /** Creates new SearchBean
     * @param directory index
     * @param queryString string to search with
     * @param querySortField field to sort on
     */
    public SearchBean(Directory directory, String queryString, String querySortField) {
        this(directory);
        setQueryString(queryString);
        setQuerySortField(querySortField);
    }

    /** Creates new SearchBean
     * @param directory index
     * @param queryString string to search with
     * @param querySortField field to sort on
     * @param queryType used to indicate which index and default Field
     */
    public SearchBean(Directory directory, String queryString, String querySortField, String queryType){
        this(directory);
        setQueryString(queryString);
        setQuerySortField(querySortField);
        setQueryType(queryType);
    }

    /** main search method
     */
    public HitsIterator search() throws IOException, ParseException{
        return search(queryString,querySortField);
    }

    /** main search method
     * @param queryString string to search with
     */
    public HitsIterator search(String queryString) throws IOException, ParseException{
        return search(queryString,queryString);
    }

    /** main search method
     * @param queryString string to search with
     * @param querySortField field to sort on
     */
    public HitsIterator search(String queryString, String querySortField) throws IOException, ParseException{
        return search(queryString, querySortField, queryType);
    }

    /** main search method
     * @param queryString string to search with
     * @param querySortField field to sort on
     * @param queryType used to indicate the index to search
     */
    public HitsIterator search(String queryString, String querySortField, String queryType)    throws IOException, ParseException {
        long startTime = System.currentTimeMillis();
        Hits hits = searchHits(queryString, queryType);

        //if (hits == null) {return null;}
        //if (hits.length() == 0) {return null;}

        HitsIterator hi = new HitsIterator(hits, querySortField);
        long endTime = System.currentTimeMillis();
        setSearchTime(endTime - startTime);
        setHitsIterator(hi);
        //searchLogger.info("queryString = "+queryString + "sort field = "+ querySortField +" #results = "+hits.length());
        return hi;
    }

    /** does the actual searching
     */
    private Hits searchHits(String queryString, String queryType) throws IOException, ParseException{
        System.out.println("queryString = " + queryString);
        if (queryString == "") {
            return null;
        }

        // Provide for multiple indices in the future

        searcher = new IndexSearcher(directory);
        Query query = getQuery(queryString, defaultSearchField);
        System.out.println("###querystring= "+query.toString(defaultSearchField));
        Hits hits = searcher.search(query);
        //System.out.println("Number hits = "+hits.length());
        //logger.debug("queryString = "+query.toString(searchField)+" hits = "+hits.length()+" queryType = "+queryType+" indexPath = "+indexPath );
        return hits;
    }

    /**
     * frees resources associated with SearchBean search
     */
    public void close() throws IOException{
        searcher.close();
    }

    /** <queryString> | <queryType> | <querySortField>
     */
    public String toString(){
        return queryString+"|"+queryType+"|"+querySortField;
    }

    /** setter for queryString
     */
    public void setQueryString
    (String queryString) {
        this.queryString = queryString;
    }

    /** getter for queryString
     */
    public String getQueryString(){
        return queryString;
    }

    /** getter for Lucene Query
     */
    private Query getQuery(String queryString, String searchField) throws ParseException {
        //String defaultSearchField = "text";
        Analyzer analyzer = new StandardAnalyzer();
        Query query = QueryParser.parse(queryString, searchField, analyzer);
        //System.out.println(query.toString());
        return query;
    }

    /** Getter for property defaulSearchField.
     * @return Value of property defaulSearchField.
     */
    public String getDefaultSearchField() {
        return defaultSearchField;
    }

    /** Setter for property defaulSearchField.
     * @param defaulSearchField New value of property defaulSearchField.
     */
    public void setDefaultSearchField(java.lang.String defaultSearchField) {
        this.defaultSearchField = defaultSearchField;
    }

    /** Getter for property searchTime.
     * @return Value of property searchTime.
     */
    public long getSearchTime() {
        return searchTime;
    }

    /** Setter for property searchTime.
     * @param searchTime New value of property searchTime.
     */
    public void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }

    /** Getter for property querySortField.
     * @return Value of property querySortField.
     */
    public java.lang.String getQuerySortField() {
        return querySortField;
    }

    /** Setter for property querySortField.
     * @param querySortField New value of property querySortField.
     */
    public void setQuerySortField(String querySortField) {
        this.querySortField = querySortField;
    }

    /** Getter for property hitsIterator.
     * @return Value of property hitsIterator.
     */
    public HitsIterator getHitsIterator() {
        return hitsIterator;
    }

    /** Setter for property hitsIterator.
     * @param hitsIterator New value of property hitsIterator.
     */
    public void setHitsIterator(HitsIterator hitsIterator) {
        this.hitsIterator = hitsIterator;
    }

    /** Getter for property queryType.
     * @return Value of property queryType.
     */
    public java.lang.String getQueryType() {
        return queryType;
    }

    /** Setter for property queryType.
     * @param queryType New value of property queryType.
     */
    public void setQueryType(java.lang.String queryType) {
        this.queryType = queryType;
    }

    /** Getter for property directory.
     * @return Value of property directory.
     */
    public org.apache.lucene.store.Directory getDirectory() {
        return directory;
    }

    /** Setter for property directory.
     * @param directory New value of property directory.
     */
    public void setDirectory(org.apache.lucene.store.Directory directory) {
        this.directory = directory;
    }

}
