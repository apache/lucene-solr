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
respectivley.

BUGS:
I tried to create a .war file for this project but I am having trouble
getting it to deploy properly.

PLANNED:
I am planning to document the result.jsp file line for line to explain
how to display a search result when you do not know what the names of 
the search fields stored in the lucene-index. That is the way the result
page is currently written.

Time permitting I want to write a couple of child tags for telling the search
tag that there are multiple index to be searched and to let it do the other
types of searches such as fuzzy and range queries.