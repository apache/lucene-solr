/*
 * Created on May 24, 2003
 */
package com.netwebapps.taglib.search;

/**
 * @company Network Web Application
 * @url http://www.netwebapps.com
 * @author Bryan LaPlante 
 */
import javax.servlet.jsp.tagext.*;

public class FieldTagTei extends TagExtraInfo
{

	public FieldTagTei(){
	}
	/*
	 * VariableInfo is provided by the servlet container and allows the
	 * FieldTag class to output it's tag variables to the PageContext at runtime
	 * @see javax.servlet.jsp.tagext.TagExtraInfo#getVariableInfo(javax.servlet.jsp.tagext.TagData)
	 */
	public VariableInfo[] getVariableInfo(TagData tagdata)
	{
		VariableInfo avariableinfo[] = new VariableInfo[1];
		avariableinfo[0] = new VariableInfo(tagdata.getId(),"com.netwebapps.taglib.search.FieldTag", true, VariableInfo.NESTED);
		return avariableinfo;
	}
}