/*
 * Created on May 24, 2003
 */
package com.netwebapps.taglib.search;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.BodyTagSupport;

/**
 * @company Network Web Application
 * @url http://www.netwebapps.com
 * @author Bryan LaPlante 
 */
public class ColumnTag extends BodyTagSupport{

	private Object parent = null;
	private Set fieldSet = null;
	private ArrayList fieldArray = new ArrayList();
	private Iterator fieldNames = null;
	private Iterator nextField = null;
	private Method getFieldsMethod = null;
	private boolean abort = false;
	public boolean throwOnException = false;
	public String columnName = "";
	public boolean runOnce = false;
	public int columnCount = 0;
	
	public int doStartTag() throws JspException{
		parent = findAncestorWithClass(this,com.netwebapps.taglib.search.SearchTag.class);
		if(runOnce && getLoopCount() > 1){
			abort = true;
			return SKIP_BODY;
		}
		try {
			getFieldsMethod = (Method) parent.getClass().getMethod("getFields",null);
			fieldSet = (Set) getFieldsMethod.invoke(parent, null);
		} catch (SecurityException e) {
			if(throwOnException){
				throw new JspException("A security violation occurred: " + e);
			}
			abort = true;
		} catch (IllegalArgumentException e) {
			if(throwOnException){
				throw new JspException("IllegalArgumentException: " + e);
			}
			abort = true;
		} catch (NoSuchMethodException e) {
			if(throwOnException){
				throw new JspException("Unable to declair the getField method : " + e);
			}
			abort = true;
		} catch (IllegalAccessException e) {
			if(throwOnException){
				throw new JspException("Access denied: " + e);
			}
			abort = true;
		} catch (InvocationTargetException e) {
			if(throwOnException){
				throw new JspException("This tag must be nested in a Search tag in order to work: " + e);
			}
			abort = true;
		}catch(NullPointerException e){
			if(throwOnException){
				throw new JspException(e);
			}
			abort = true;
		}
		
		if(abort){
			return SKIP_BODY;
		}
		
		if(fieldSet != null){
			nextField = fieldSet.iterator();
			while(nextField.hasNext()){
				fieldArray.add(nextField.next());
			}
			columnCount = fieldSet.size();
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return EVAL_BODY_AGAIN;
		}

		return SKIP_BODY;
	}
	
	public void doInitBody() throws JspException{
		if(!abort){
			if (fieldArray.size() > 0) {
				fieldNames = fieldArray.iterator();
				if(fieldNames.hasNext()){
					columnName = (String) fieldNames.next();
					columnCount = fieldSet.size();
					pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
				}
			}
		}
	}
	
	public int doAfterBody() throws JspException{
		if(abort){
			return SKIP_BODY;
		}
		columnName = "";

		try{
			getBodyContent().writeOut(getPreviousOut());
			getBodyContent().clearBody();
		}
		catch(IOException e){
			throw new JspException(e.toString());
		}
		if(fieldNames != null){
			if(fieldNames.hasNext()){
				columnName = (String) fieldNames.next();
				columnCount = fieldSet.size();
				pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
				return EVAL_BODY_AGAIN;
			}
		}
		return SKIP_BODY;
	}
	
	public void release(){
		parent = null;
		fieldSet = null;
		fieldArray = null;
		fieldNames = null;
		nextField = null;
		getFieldsMethod = null;
	}
	
	private int getLoopCount() throws JspException{
		Field getLoopCountMember = null;
		int rc = 0; 
		try {
			getLoopCountMember = (Field) parent.getClass().getField("loopCount");
			rc = new Integer(getLoopCountMember.get(parent).toString()).intValue();
		} catch (SecurityException e) {
			if(throwOnException){
				throw new JspException("A security violation occurred: " + e);
			}
		} catch (NoSuchFieldException e) {
			if(throwOnException){
				throw new JspException("Unable to find the loopCount field : " + e);
			}
		}catch(IllegalAccessException e){
			if(throwOnException){
				throw new JspException("Access denied: " + e);
			}
		}catch(IllegalArgumentException e){
			if(throwOnException){
				throw new JspException("Bad argument: " + e);
			}
		}
		return rc;
	}

	/**
	 * @param string
	 */
	public void setcolumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * @param b
	 */
	public void setThrowOnException(String b) {
		throwOnException = new Boolean(b).booleanValue();
	}
	public void setThrowOnException(boolean b) {
		throwOnException = b;
	}
	
	public void setRunOnce(boolean b){
		runOnce = b;		
	}
	
	public void setRunOnce(String b){
		runOnce = new Boolean(b).booleanValue();	
	}

}
