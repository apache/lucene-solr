<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/Document">
<!--This template ANDs all fields together. Within a single field all terms are ORed.
	The query fields are fed directly through an analyzer and so do not need to adhere to  
	traditional Lucene query syntax.
 -->	
<BooleanQuery>
	<xsl:if test="count(artist)>0">
	    <Clause occurs="must">
	      <TermsQuery fieldName="artist"><xsl:value-of select="artist"/></TermsQuery>
	   </Clause>
   </xsl:if>
	<xsl:if test="count(album)>0">
	    <Clause occurs="must">
	      <TermsQuery fieldName="album"><xsl:value-of select="album"/></TermsQuery>
	   </Clause>
   </xsl:if>
	<xsl:if test="count(genre)>0">
	    <Clause occurs="must">
	      <TermsQuery fieldName="genre"><xsl:value-of select="genre"/></TermsQuery>
	   </Clause>
   </xsl:if>
	<xsl:if test="count(releaseDate)>0">
	    <Clause occurs="must">
	      <TermsQuery fieldName="releaseDate"><xsl:value-of select="releaseDate"/></TermsQuery>
	   </Clause>
   </xsl:if>
</BooleanQuery>

</xsl:template>
</xsl:stylesheet>