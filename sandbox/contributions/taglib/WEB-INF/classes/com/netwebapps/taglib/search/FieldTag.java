/*
 * Created on May 23, 2003
 *
 */
package com.netwebapps.taglib.search;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.TagSupport;

/**
 * @company Network Web Application
 * @url http://www.netwebapps.com
 * @author Bryan LaPlante 
 */
public class FieldTag extends TagSupport{
	
	public String name = "";
	public boolean throwOnException = false;
	public String value = "";
	private boolean abort = false;
	
	/* (non-Javadoc)
	 * @see javax.servlet.jsp.tagext.BodyTagSupport#doEndTag()
	 */
	public int doStartTag() throws JspException {
		Object parent = findAncestorWithClass(this,com.netwebapps.taglib.search.SearchTag.class);
		try {
			Method getFieldMethod = parent.getClass().getMethod("getField", new Class[] {Class.forName("java.lang.String")});
			value = getFieldMethod.invoke(parent, new String[] {name}).toString();
		} catch (SecurityException e) {
			if(throwOnException){
				throw new JspException("A security violation occurred: " + e);
			}
			abort = true;
		} catch (NoSuchMethodException e) {
			if(throwOnException){
				throw new JspException("Unable to declair the getField method : " + e);
			}
			abort = true;
		} catch (ClassNotFoundException e) {
			if(throwOnException){
				throw new JspException("ClassNotFoundException: " + e);
			}
		}catch (IllegalAccessException e) {
			if(throwOnException){
				throw new JspException("Access denied: " + e);
			}
			abort = true;
		}catch (InvocationTargetException e) {
			if(throwOnException){
				throw new JspException("This tag must be nested in a Search tag in order to work: " + e);
			}
			abort = true;
		}catch(NullPointerException e){
			if(throwOnException){
				throw new JspException("This tag must be nested in a Search tag in order to work: " + e);
			}
			abort = true;
		}

		if(abort){
			pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
			return SKIP_BODY;
		}
		pageContext.setAttribute(getId(),this,PageContext.PAGE_SCOPE);
		return EVAL_BODY_INCLUDE;
	}
	
	public void release(){
		name = "";
		throwOnException = false;
		value = "";
	}

	/**
	 * @param string
	 */
	public void setName(String string) {
		name = string;
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

}
