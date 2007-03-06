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

# TODO Add sorting
class FlareContext
  attr_accessor :queries, :filters, :facet_queries, :applied_facet_queries
  attr_reader :facet_fields, :text_fields

  def initialize(solr_config)
    @connection = Solr::Connection.new(solr_config[:solr_url])

    clear
    @facet_queries = {}  # name => {:queries => [], :filters => []}
    
    puts "initialize\n-------","#{solr_config.inspect}"
    @index_info = @connection.send(Solr::Request::IndexInfo.new)

    @facet_fields = @index_info.field_names.find_all {|v| v =~ /_facet$/}
    @text_fields = @index_info.field_names.find_all {|v| v =~ /_text$/}
  end
  
  def clear
    puts "clear\n-------"
    @queries = []
    @filters = []
    @applied_facet_queries = []

    # this is cleared for development purposes - allowing flare to stay running but different Solr datasets swapping
    @index_info = @connection.send(Solr::Request::IndexInfo.new)
    @facet_fields = @index_info.field_names.find_all {|v| v =~ /_facet$/}
    @text_fields = @index_info.field_names.find_all {|v| v =~ /_text$/}
    
    # facet_queries not cleared as their lifetime is different than constraints
  end
    
  def search(start, max)
    facet_queries = @facet_queries.collect do |k,v|
      clauses = filter_queries(v[:filters])
      clauses << build_boolean_query(v[:queries])
      query = clauses.join(" AND ")
      @facet_queries[k][:real_query] = query
      query
    end
    
    qa = applied_facet_queries.collect {|map| q = @facet_queries[map[:name]][:real_query]; map[:negative] ? "-(#{q})" : q}
    qa << build_boolean_query(@queries)
    request = Solr::Request::Standard.new(:query => qa.join(" AND "),
                                          :filter_queries => filter_queries(@filters),
                                          :start => start,
                                          :rows => max,
                                          :facets => {
                                            :fields => @facet_fields, :limit => 20 , :mincount => 1, :sort => :count,
                                            :queries => facet_queries
                                          },
                                          :highlighting => {:field_list => @text_fields})
    
    #TODO: call response.field_facets(??) - maybe field_facets should be return a higher level? 
#    logger.info({:query => query, :filter_queries => filters}.inspect)
    @connection.send(request)
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
