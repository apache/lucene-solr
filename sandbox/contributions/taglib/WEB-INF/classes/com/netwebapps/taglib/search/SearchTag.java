package com.netwebapps.taglib.search;

import java.io.File;
import java.io.IOException;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.BodyTagSupport;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Query;

/*
 *
 * @company Network Web Application
 * @url http://www.netwebapps.com
 * @author Bryan LaPlante
 *
 */
public class SearchTag extends BodyTagSupport{

	private HashMap hitMap = null;
	private ArrayList hitArray = null;
	private IndexSearcher searcher = null;
	private Query query = null;
	private Hits hits = null;
	private int thispage = 0;
	private String criteria = "";
	private Iterator searchItr = null;
	private Enumeration fields = null;
	private HashMap aField = new HashMap();
	private int ROWCOUNT = 0;
	private int PAGECOUNT = 0;
	private int HITCOUNT = 0;
	private boolean abort = false;
	private Analyzer analyzer = null;
	private Document doc = null;
	private ArrayList idxArray = new ArrayList();
	private MultiSearcher msearcher = null;
	private final int GERMAN_ANALYZER = 0;
	private final int SIMPLE_ANALYZER = 1;
	private final int STANDARD_ANALYZER = 2;
	private final int STOP_ANALYZER = 3;
	private final int WHITESPACE_ANALYZER = 4;

	public int startRow = 0;
	public int maxRows = 50;
	public int rowCount = 0;
	public int pageCount = 1;
	public int hitCount = 0;
	public int loopCount = 0;
	public String firstPage = "";
	public String nextPage = "";
	public String previousPage = "";
	public String lastPage = "";
	public LinkedList pageList = new LinkedList();
	public boolean throwOnException = false;
	public String[] stopWords = new String[0];
	public String[] fieldList = new String[0];
	public int[] flagList = new int[0];
	public String search = "contents";
	public int analyzerType = STANDARD_ANALYZER;


	public int doStartTag() throws JspException{
		rowCount = startRow + ROWCOUNT++;
		loopCount++;
		pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
		return EVAL_BODY_AGAIN;
	}

	public void doInitBody() throws JspException{
		doSearch();
		if(!abort){
			searchItr = hitArray.iterator();
			if(searchItr.hasNext()){
				aField = (HashMap) searchItr.next();
				rowCount = startRow + ROWCOUNT++;
				pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			}
		}
	}

	public int doAfterBody() throws JspException{

		if(abort){
			hitCount = 0;
			loopCount = 0;
			rowCount = startRow + ROWCOUNT;
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return SKIP_BODY;
		}

		try{
			getBodyContent().writeOut(getPreviousOut());
			getBodyContent().clearBody();
		}
		catch(IOException e){
			throw new JspException(e.toString());
		}

		if(searchItr.hasNext()){
			aField = (HashMap) searchItr.next();
			rowCount = startRow + ROWCOUNT++;
			loopCount++;
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return EVAL_BODY_AGAIN;
		}
		return SKIP_BODY;
	}

	public int doEndTag() throws JspException{

		if(abort){
			hitCount = 0;
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return EVAL_PAGE;
		}

		try{
			HttpServletRequest req = (HttpServletRequest) pageContext.getRequest();
			String relativePath = req.getRequestURI();
			firstPage = relativePath + "?startRow=0&maxRows=" + maxRows;
			nextPage = relativePath + "?startRow=" + ((startRow + maxRows <= HITCOUNT)? startRow + maxRows : startRow) + "&maxRows=" + maxRows;
			previousPage = relativePath + "?startRow=" + ((startRow - maxRows >=0)? startRow - maxRows : 0 ) + "&maxRows=" + maxRows;
			lastPage = relativePath + "?startRow=" + (((HITCOUNT - maxRows) >= 0)? HITCOUNT - maxRows : 0) + "&maxRows=" + maxRows;
			if(HITCOUNT > 0){
				pageList = new LinkedList();
				for(int i=0; i < (HITCOUNT / maxRows); i++){
					String tempURL = relativePath + "?startRow=" + (maxRows * i) + "&maxRows=" + maxRows;
					pageList.add(tempURL);
				}
			}
		}
		catch(Exception e){
			throw new JspException("A problem occured durring doEndTag: " + e.toString());
		}

		pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
		return EVAL_PAGE;
	}

	public void release(){
		hitMap = null;
		hitArray = null;
		searcher = null;
		query = null;
		hits = null;
		thispage = 0;
		criteria = "";
		searchItr = null;
		fields = null;
		aField = new HashMap();
		ROWCOUNT = 0;
		PAGECOUNT = 1;
		HITCOUNT = 0;
		abort = false;
		analyzer = null;
		doc = null;
		idxArray = null;
		msearcher = null;
	}

	public String getField(String name){
		if(aField != null){
			if(aField.containsKey(name)){
				return aField.get(name).toString();
			}
		}
		return "";
	}

	public Set getFields(){
		return aField.keySet();
	}


	public void addCollection(String name) throws JspException{
		try {
			searcher = new IndexSearcher(IndexReader.open(name));
			idxArray.add(searcher);
		} catch (IOException e) {
			if(throwOnException){
				throw new JspException("Error occured while opening " + name + " ]: " + e);
			}
		}
	}

	public void doSearch() throws JspException{

		try {
			if(idxArray.size() > 0){
				IndexSearcher[] idxToArray = new IndexSearcher[idxArray.size()];
				Iterator idxIter = idxArray.iterator();
				int arrayCount = 0;
				while(idxIter.hasNext()){
					idxToArray[arrayCount++] = (IndexSearcher) idxIter.next();
				}
				msearcher = new MultiSearcher(idxToArray);
			}else{
				throw new JspException("No collection has been specified");
			}
		} catch (IOException e) {
			if(throwOnException){
				throw new JspException("IndexSearcher(IndexReader.open(collection)): " + e);
			}
			abort = true;
		}
		if(!abort){
			// choosing the type of analyzer to use in this search
			switch (analyzerType) {
				case GERMAN_ANALYZER:
					if(stopWords.length > 0){
						analyzer = new GermanAnalyzer(stopWords);
					}else{
						if(throwOnException){
							throw new JspException("In order to use a GermanAnalyzer you must provide a list of stop words");
						}
						abort = true;
					}
					break;
				case SIMPLE_ANALYZER:
					analyzer = new SimpleAnalyzer();
					break;
				case STANDARD_ANALYZER:
					if(stopWords.length > 0){
						analyzer = new StandardAnalyzer(stopWords);
					}else{
						analyzer = new StandardAnalyzer();
					}
					break;
				case STOP_ANALYZER:
					if(stopWords.length > 0){
						analyzer = new StopAnalyzer(stopWords);
					}else{
						analyzer = new StopAnalyzer();
					}
					break;
				case WHITESPACE_ANALYZER:
					analyzer = new WhitespaceAnalyzer();
					break;

				default :
				if(stopWords.length > 0){
					analyzer = new StandardAnalyzer(stopWords);
				}else{
					analyzer = new StandardAnalyzer();
				}
					break;
			}

			try {
				// choose a query parser
				if(fieldList.length > 0){
					if(flagList.length > 0){
						query = MultiFieldQueryParser.parse(criteria,fieldList,flagList,analyzer);
					}else{
						query = MultiFieldQueryParser.parse(criteria,fieldList,analyzer);
					}
				}else{
					query = QueryParser.parse(criteria, search, analyzer);
				}
			} catch (ParseException e) {
				if(throwOnException){
					throw new JspException("If using fieldList and or flagList check to see you have the same number of items in each: " + e);
				}
				abort = true;
			}
			if(!abort){
				try {
					hits = msearcher.search(query);
				} catch (IOException e) {
					if(throwOnException){
						throw new JspException("msearcher.search(query): " + e);
					}
					abort = true;
				}

				if(!abort){
					hitCount = hits.length();
					HITCOUNT = hits.length();
					PAGECOUNT = (int) (( (double) startRow) / maxRows );
					pageCount = PAGECOUNT;
					thispage = maxRows;
					if ((startRow + maxRows) > hits.length()) {
							thispage = hits.length() - startRow;
					}
					hitArray = new ArrayList();
					for (int i = startRow; i < (thispage + startRow); i++) {
						hitMap = new HashMap();
						try {
							doc = hits.doc(i);
						} catch (IOException e) {
							if(throwOnException){
								throw new JspException("hits.doc(i) : " + e);
							}
							abort = true;
						}
						if(!abort){
							try {
								hitMap.put("score",new Float(hits.score(i)).toString());
							} catch (IOException e) {
								if(throwOnException){
									throw new JspException("hitMap.put(score,new Float(hits.score(i)).toString()); : " + e);
								}
								abort = true;
							}
							if(!abort){
								fields = doc.fields();
								while(fields.hasMoreElements()){
									Field field = (Field) fields.nextElement();
									String fieldName = field.name();
									hitMap.put(fieldName,doc.get(fieldName));
								}
								hitArray.add(hitMap);
							}
						}
					}
				}
			}
		}
		if(msearcher != null){
			try {
				msearcher.close();
			} catch (IOException e) {
				if(throwOnException){
					throw new JspException("A problem occured trying to close the searcher : " + e);
				}
			}
		}
	}

	public void setCriteria(String criteria){
		this.criteria = criteria;
	}

	public void setStartRow(String startRow){
		try{
			this.startRow = Integer.parseInt(startRow);
		}
		catch(Exception e){
			this.startRow = 0;
		}
	}

	public void setStartRow(int startRow){
		this.startRow = startRow;
	}

	public void setMaxRows(String maxRows){
		try{
			this.maxRows = Integer.parseInt(maxRows);
		}
		catch(Exception e){
			this.maxRows = 10;
		}
	}

	public void setMaxRows(int maxRows){
		this.maxRows = maxRows;
	}

	public void setCollection(String collection) throws JspException{
		idxArray = new ArrayList();
		String[] collectionArray = collection.split(",");
		for(int i=0; i<collectionArray.length; i++){
			this.addCollection(collectionArray[i]);
		}
	}

	public void setThrowOnException(String bool){
		this.throwOnException = new Boolean(bool).booleanValue();
	}
	public void setThrowOnException(boolean b) {
		throwOnException = b;
	}

	public int getStartRow(){
		return startRow;
	}

	public int getMaxRows(){
		return maxRows;
	}

	public void setStopWords(String swords) throws JspException{
		Hashtable wordTable = new Hashtable();
		String[] temp = new String[wordTable.size()];
		if(swords.split(",").length > 0){
			String[] words = swords.split(",");
			for (int i = 0; i < words.length; i++) {
				if(new File(words[i]).isFile()){
					wordTable.putAll(WordlistLoader.getWordtable(words[i]));
				}else{
					wordTable.put(words[i], words[i]);
				}
			}
			temp = new String[wordTable.size()];

			int count = 0;
			if(wordTable.size() > 0){
				Iterator wtIter = wordTable.keySet().iterator();
				while (wtIter.hasNext()){
					temp[count++] = (String) wtIter.next();
				}
			}
		}
		stopWords = temp;
	}

//	public void setStopWords(String[] swords) throws JspException{
//		stopWords = swords;
//	}

	public void setFlagList(String fg) {
		int[] list = new int[0];
		if(fg.split(",").length > 0){
			String[] ssplit = fg.split(",");
			Integer fsplit = new Integer(fg.split(",").length);
			list = new int[fsplit.intValue()];
			for(int i=0; i < fsplit.intValue(); i++){
				if(ssplit[i].equalsIgnoreCase("NORMAL")){
					list[i] = MultiFieldQueryParser.NORMAL_FIELD;
				}else if(ssplit[i].equalsIgnoreCase("PROHIBITED")){
					list[i] = MultiFieldQueryParser.PROHIBITED_FIELD;
				}else if(ssplit[i].equalsIgnoreCase("REQUIRED")){
					list[i] = MultiFieldQueryParser.REQUIRED_FIELD;
				}
			}
		}
		flagList = list;
	}

	public void setFieldList(String fl) {
		if(fl.split(",").length > 0){
			fieldList = fl.split(",");
		}
	}

	public void setFieldList(String[] fl) {
		fieldList = fl;
	}

	public void setSearch(String string) {
		search = string;
	}

	/**
	 * @param atype
   * @todo this is crying for constants, not string comparisons
	 */
	public void setAnalyzerType(String atype) {
		if(atype.equalsIgnoreCase("GERMAN_ALYZER")){
			analyzerType = 0;
		}else if(atype.equalsIgnoreCase("SIMPLE_ANALYZER")){
			analyzerType = 1;
		}else if(atype.equalsIgnoreCase("STANDARD_ANALYZER")){
			analyzerType = 2;
		}else if(atype.equalsIgnoreCase("STOP_ANALYZER")){
			analyzerType = 3;
		}else if(atype.equalsIgnoreCase("WHITESPACE_ANALYZER")){
			analyzerType = 4;
		}else{
			analyzerType = 2;
		}
	}

}
