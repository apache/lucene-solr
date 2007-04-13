# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'solr'

class Flare::Context
  attr_accessor :queries, :filters, :facet_queries, :applied_facet_queries, :page, :sort
  attr_reader :facet_fields, :text_fields

  def initialize(solr_config={})
    @solr_config = solr_config
    
    @connection = Solr::Connection.new(@solr_config[:solr_url])

    clear
    @facet_queries = {}  # name => {:queries => [], :filters => []}

    @index_info = @connection.send(Solr::Request::IndexInfo.new)

    excluded =  @solr_config[:facets_exclude] ? @solr_config[:facets_exclude].collect {|e| e.to_s} : []
    @facet_fields =  @index_info.field_names.find_all {|v| v =~ /_facet$/} - excluded  # TODO: is facets_excluded working?  where are the tests?!  :)

    @text_fields = @index_info.field_names.find_all {|v| v =~ /_text$/}
    
    @page = 1
  end

  def clear
    #TODO unify initialize and clear
    @queries = []
    @filters = []
    @applied_facet_queries = []
    @page = 1

    # this is cleared for development purposes - allowing flare to stay running but different Solr datasets swapping
    @index_info = @connection.send(Solr::Request::IndexInfo.new)
    excluded =  @solr_config[:facets_exclude] ? @solr_config[:facets_exclude].collect {|e| e.to_s} : []
    @facet_fields =  @index_info.field_names.find_all {|v| v =~ /_facet$/} - excluded
    @text_fields = @index_info.field_names.find_all {|v| v =~ /_text$/}

    # facet_queries not cleared as their lifetime is different than constraints
  end

  def empty_constraints?
    @queries.empty? && @filters.empty? && @applied_facet_queries.empty?
  end

  def search(start=0, max=25)
    facet_queries = @facet_queries.collect do |k,v|
      clauses = filter_queries(v[:filters])
      clauses << build_boolean_query(v[:queries])
      query = clauses.join(" AND ")
      @facet_queries[k][:real_query] = query
      query
    end

    qa = applied_facet_queries.collect {|map| q = @facet_queries[map[:name]][:real_query]; map[:negative] ? "-(#{q})" : q}
    qa << build_boolean_query(@queries)
    
    query_type = @solr_config[:solr_query_type] || :dismax
    query_config = @solr_config["#{query_type.to_s}_query_params".to_sym] || {}
    solr_params = query_config.merge(:query => qa.join(" AND "),
                                     :filter_queries => filter_queries(@filters),
                                     :start => start,
                                     :rows => max,
                                     :facets => {
                                       :fields => @facet_fields, :limit => 20 , :mincount => 1, :sort => :count,
                                       :queries => facet_queries
                                     },
                                     :highlighting => {:field_list => @text_fields},
                                     :sort => @sort)
#    if query_type == :dismax
#      solr_params[:phrase_fields] ||= @text_fields
#      if solr_params[:query] == "*:*"
#        solr_params[:query] = ""
#      end
#      request = Solr::Request::Dismax.new(solr_params)  # TODO rename to DisMax
#    else
      request = Solr::Request::Standard.new(solr_params)
#    end

    #TODO: call response.field_facets(??) - maybe field_facets should be higher level? 
#    logger.info({:query => query, :filter_queries => filters}.inspect)
    @connection.send(request)
  end
  
  def document_by_id(id)
    request = Solr::Request::Standard.new(:query => "id:\"#{id}\"")
    @connection.send(request).hits[0]
  end

  def retrieve_field_facets(field, limit=-1, prefix=nil)
    req = Solr::Request::Standard.new(:query => build_boolean_query(@queries),
       :filter_queries => filter_queries(@filters),
       :facets => {:fields => [field],
                   :mincount => 1, :limit => limit, :prefix => prefix, :missing => true, :sort => :count
                  },
       :rows => 0
    )

    results = @connection.send(req)

    results.field_facets(field)
  end

  def to_s
    <<-TO_S
    ------
    Applied facet queries: #{applied_facet_queries.inspect}
    Queries: #{queries.inspect}
    Filters: #{filters.inspect}
    Facet queries: #{facet_queries.inspect}
    ------
    TO_S
  end

  private
  def build_boolean_query(queries)
    if queries.nil? || queries.empty?
      query = "*:*"
    else
      query = queries.collect{|q| "#{q[:negative] ? '-' : ''}(#{q[:query]})"}.join(' AND ')
    end

    query
  end

  def filter_queries(filters)
    filters.collect do |filter|
      value = filter[:value]
      if value != "[* TO *]"
        value = "\"#{value}\""
      end
      "#{filter[:negative] ? '-' : ''}#{filter[:field]}:#{value}"
    end
  end
end
