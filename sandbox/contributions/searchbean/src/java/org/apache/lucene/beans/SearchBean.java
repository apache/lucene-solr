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
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;

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

    /** Creates new SearchBean
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

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String getQueryString(){
        return queryString;
    }

    /** Parses the query
     * @todo allow for user defined analyzer
     */
    private Query getQuery(String queryString, String searchField) throws ParseException {
        //String defaultSearchField = "text";
        Analyzer analyzer = new StandardAnalyzer();
        Query query = QueryParser.parse(queryString, searchField, analyzer);
        //System.out.println(query.toString());
        return query;
    }

    public String getDefaultSearchField() {
        return defaultSearchField;
    }

    public void setDefaultSearchField(String defaultSearchField) {
        this.defaultSearchField = defaultSearchField;
    }

    public long getSearchTime() {
        return searchTime;
    }

    public void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }

    public java.lang.String getQuerySortField() {
        return querySortField;
    }

    public void setQuerySortField(String querySortField) {
        this.querySortField = querySortField;
    }

    public HitsIterator getHitsIterator() {
        return hitsIterator;
    }

    public void setHitsIterator(HitsIterator hitsIterator) {
        this.hitsIterator = hitsIterator;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }

}
