<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/Document">
<!-- This template uses an efficient, cached filter for the "genre" field".
	Other query fields are fed directly through an analyzer and so do not need to adhere to  
	traditional Lucene query syntax. Terms within a field are ORed while different fields are ANDed
 -->	
<FilteredQuery>
	<Query>
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
			<xsl:if test="count(releaseDate)>0">
			    <Clause occurs="must">
			      <TermsQuery fieldName="releaseDate"><xsl:value-of select="releaseDate"/></TermsQuery>
			   </Clause>
		   </xsl:if>
	</BooleanQuery>
	</Query>
	<Filter>
		<CachedFilter>
			<!-- Example filter to be cached for fast, repeated use -->
			<TermsFilter fieldName="genre">			
				<xsl:value-of select="genre"/>
			</TermsFilter>
		</CachedFilter>		
	</Filter>	
</FilteredQuery>
</xsl:template>
</xsl:stylesheet>