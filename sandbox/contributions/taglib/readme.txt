INTRODUCTION
The lucene-taglib project provides a tag library for searching
a lucene-index.

INSTRUCTIONS
1. download this project and create a context in your favorite
	servelt container called lucene-taglib.
2.	copy the file under the taglib directory into your new context.
3.	open result.jsp and locate the collection attribute of the 
	<LUCENE:Search> tag.
4.	change the collection attribute to point to a lucene-index
	that you created using the system appropriate path.
5.	open index.jsp in your browser and enter search criteria 
	and click the submit button.
	
DOCUMENTATION:
you will find documentation and an over view of the tag library in
the docs folder of this project and the javadocs in the api folder
respectively.

BUGS:
I tried to create a .war file for this project but I am having trouble
getting it to deploy properly.

More like a heads up than a bug I discovered that if you have tag pooling \
turned on in Tomcat that values passed to a custom tag will not change in
the event that an exception is thrown. Catching the exception inside of
the tag or page does not help, to update the values at this point you
have to turn off tag pooling. Follow the instructions below to do this
in Tomcat.

----------------------------------------------------------------------------

  From: Bill Barker
  Subject: Re: HELP:Tagpool sharing problems
  Date: Mon, 28 Apr 2003 22:20:20 -0700

----------------------------------------------------------------------------

In $CATALINA_HOME/conf/web.xml, locate the <servlet-name>jsp</servlet-name>
servlet, and add:
  <init-param>
    <param-name>enablePooling</param-name>
    <param-value>false</param-value>
  </init-param>

This will turn off tag-pooling.  You'll also need to clear out
$CATALINA_HOME/work so that the JSP pages get re-compiled.

If you just want it turned off for one context, then you can place the
definition of the jsp servlet in your own web.xml.

If you are using:
  <servlet>
    <servlet-name>myJspPage</servlet-name>
    <jsp-page>myJspPage.jsp</jsp-page>
  </servlet>
then you also need to add the enablePooling init-param to your servlet's
definition.


HISTORY:
1.	Added more robust error handling and the ability to turn it on and
	off with a throwOnException attribute. (All tags)
2.	Added a Column tag for outputting the field names found in a 
	Lucene index.
3.	Added a Field tag for retrieving a value for a field in a search
	result either produced by the Column tag or known in advance.
4.	Added new example pages to illustrate how to use the new tags
5.	The Collection tag has been deprecated, use the collection attribute
	of the Search tag instead.