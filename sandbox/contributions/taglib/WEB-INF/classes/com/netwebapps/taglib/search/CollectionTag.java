/*
 * Created on May 18, 2003
 *
 */
package com.netwebapps.taglib.search;

import java.lang.reflect.Method;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;
;

/**
 * @author blaplante
 *
 */
public class CollectionTag extends TagSupport{
	String directory = "";
	/* (non-Javadoc)
	 * @see javax.servlet.jsp.tagext.Tag#doStartTag()
	 */
	public int doStartTag() throws JspException {
		Object parent = getParent();
		if(parent != null){
			try{
				Method call = parent.getClass().getMethod("addCollection", new Class[] {Class.forName("java.lang.String")});
				call.invoke(parent, new String[] {directory});
			}
			catch(Exception e){
				throw new JspException("An error occured while trying to add a new collection path: " + e.getCause());
			}
		}
		return SKIP_BODY;
	}

	/* (non-Javadoc)
	 * @see javax.servlet.jsp.tagext.Tag#release()
	 */
	public void release() {
		directory = null;
	}
	/**
	 * @param string
	 */
	public void setDirectory(String dir) {
		this.directory = dir;
	}

}
