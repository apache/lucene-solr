package com.netwebapps.taglib.search;

/**
 * @author Network Web Application
 * @author Bryan LaPlante 
 *
 */
import javax.servlet.jsp.tagext.*;

public class SearchTagTei extends TagExtraInfo
{

    public SearchTagTei(){
    }
	/*
	 * VariableInfo is provided by the servlet container and allows the
	 * SearchTag class to output it's tag variables to the PageContext at runtime
	 * @see javax.servlet.jsp.tagext.TagExtraInfo#getVariableInfo(javax.servlet.jsp.tagext.TagData)
	 */
    public VariableInfo[] getVariableInfo(TagData tagdata)
    {
        VariableInfo avariableinfo[] = new VariableInfo[1];
        avariableinfo[0] = new VariableInfo(tagdata.getId(),"com.netwebapps.taglib.search.SearchTag", true, VariableInfo.AT_BEGIN);
        return avariableinfo;
    }
}