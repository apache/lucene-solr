<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/Document">
<BooleanQuery>
	<!-- Clause if user selects a preference for type of job - apply choice of 
	     permanent/contract filter and cache -->
	<xsl:if test="type">
	    <Clause occurs="must">
	    	<ConstantScoreQuery>
	    	   <CachedFilter>
	    	     	<TermsFilter fieldName="type"><xsl:value-of select="type"/></TermsFilter>
	    	   </CachedFilter>
	    	 </ConstantScoreQuery>
	   </Clause>
	</xsl:if>
		
	<!-- Use standard Lucene query parser for any job description input -->
	<xsl:if test="description">
		<Clause occurs="must">
			<UserQuery fieldName="description"><xsl:value-of select="description"/></UserQuery>
		</Clause>
	</xsl:if>      
	
	<!-- If any of the location fields are set OR them ALL in a Boolean filter and cache individual filters -->
	<xsl:if test="South|North|East|West">
		<Clause occurs="must">
			<ConstantScoreQuery>
				<BooleanFilter>
					<xsl:for-each select="South|North|East|West">
					<Clause occurs="should">
						<CachedFilter>
							<TermsFilter fieldName="location"><xsl:value-of select="name()"/></TermsFilter>
						</CachedFilter>
				  	</Clause>
					</xsl:for-each>					
				</BooleanFilter>
	     		</ConstantScoreQuery>
	 	</Clause>
	</xsl:if>   	
	
	<!-- Use XSL functions to split and zero pad salary range value -->
	<xsl:if test="salaryRange">
		<Clause occurs="must">
			<ConstantScoreQuery>
				<RangeFilter fieldName="salary" >
					<xsl:attribute name="lowerTerm">
						<xsl:value-of select='format-number( substring-before(salaryRange,"-"), "000" )' />
					</xsl:attribute> 
					<xsl:attribute name="upperTerm">
						<xsl:value-of select='format-number( substring-after(salaryRange,"-"), "000" )' />
					</xsl:attribute> 
				</RangeFilter>
			</ConstantScoreQuery>
		</Clause>
	</xsl:if>	
</BooleanQuery>
</xsl:template>
</xsl:stylesheet>