package com.netwebapps.taglib.search;

import java.util.*;
import javax.servlet.jsp.*;
import javax.servlet.jsp.tagext.*;
import javax.servlet.http.*;
import java.io.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

/*
 * 
 * @author Bryan LaPlante
 * @param 
 *
 */
public class SearchTag extends BodyTagSupport{
	
	private HashMap hitMap = null;
	private ArrayList hitArray = null;
	private String collection = "";
	private IndexSearcher searcher = null;
	private Query query = null;
	private Hits hits = null;
	private int thispage = 0;
	private String criteria = ""; 
	private Iterator searchItr = null;
	private Enumeration fields = null;
	private HashMap aField = new HashMap();
	private int ROWCOUNT = 0;
	private int PAGECOUNT = 1;
	private int HITCOUNT = 0;
	private boolean abort = false;
	private Analyzer analyzer = null;

	public int startRow = 0;
	public int maxRows = 50;  
	public String rowCount = "0";
	public String pageCount = "1";
	public String hitCount = "0";
	public String firstPage = "";
	public String nextPage = "";
	public String previousPage = "";
	public String lastPage = "";
	public LinkedList pageList = new LinkedList();
	public boolean throwOnException = false;
	
	
	public int doStartTag() throws JspException{
		
		doSearch();
		if(abort){
			rowCount = new Integer(startRow + ROWCOUNT).toString();
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return SKIP_BODY;
		}
		searchItr = hitArray.iterator();
		if(searchItr.hasNext()){
			aField = (HashMap) searchItr.next();
			rowCount = new Integer(startRow + ROWCOUNT++).toString();
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return EVAL_BODY_AGAIN;
		}
		return SKIP_BODY; 
	}
	
	public void doInitBody() throws JspException{
		if(!abort){
			doSearch();
			searchItr = hitArray.iterator();
			if(searchItr.hasNext()){
				aField = (HashMap) searchItr.next();
				rowCount = new Integer(startRow + ROWCOUNT).toString();
				pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			}
		}
	}
	
	public int doAfterBody() throws JspException{
		
		if(abort){
			rowCount = new Integer(startRow + ROWCOUNT).toString();
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
			rowCount = new Integer(startRow + ROWCOUNT++).toString();
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return EVAL_BODY_AGAIN;
		}
		return SKIP_BODY;
	}
	
	public int doEndTag() throws JspException{

		if(abort){
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
	}
	
	public String getField(String name){
		if(aField != null){
			if(aField.containsKey(name)){
				return aField.get((String) name).toString();
			}
		}
		return "";
	}
	
	public Set getFields(){
		return aField.keySet();
	}
	
	public void doSearch() throws JspException{

		try {
			searcher = new IndexSearcher(IndexReader.open(collection));
		} catch (IOException e) {
			if(throwOnException){
				throw new JspException("IndexSearcher(IndexReader.open(collection)): " + e);
			}
			abort = true;
		}
		if(!abort){
			analyzer = new StopAnalyzer();

			try {
				query = QueryParser.parse(criteria, "contents", analyzer);
			} catch (ParseException e) {
				if(throwOnException){
					throw new JspException("QueryParser.parse(criteria,contents,analyzer): " + e);
				}
				abort = true;
			}
			if(!abort){
				try {
					hits = searcher.search(query);
				} catch (IOException e) {
					if(throwOnException){
						throw new JspException("searcher.search(query): " + e);
					}
					abort = true;
				}
		
				if(!abort){
					hitCount = new Integer(hits.length()).toString();
					HITCOUNT = hits.length();
					PAGECOUNT = PAGECOUNT = (int) (( (double) startRow) / maxRows );
					pageCount = new Integer(PAGECOUNT).toString();
					thispage = maxRows;
					if ((startRow + maxRows) > hits.length()) {
							thispage = hits.length() - startRow;
					}
					hitArray = new ArrayList();
					for (int i = startRow; i < (thispage + startRow); i++) {
						hitMap = new HashMap();
						Document doc = null;
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
	}
	
	/* setters */
	
	
	
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
	
	public void setCollection(String collection){
		this.collection = collection;
	}
	
	public void setThrowOnException(String bool){
		this.throwOnException = new Boolean(bool).booleanValue();
	}
	
	/* getters */
	
	public int getStartRow(){
		return startRow;
	}
	
	public int getMaxRows(){
		return maxRows;
	}
}
