<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/Document">
<!-- This template is designed to work with a google-like search form - one edit box and 
	uses the traditional Lucene query syntax
 -->		
<BooleanQuery>
    <Clause occurs="must">
	      <UserQuery><xsl:value-of select="queryString"/></UserQuery>
   </Clause>
</BooleanQuery>
</xsl:template>
</xsl:stylesheet>