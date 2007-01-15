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

module Solr
  module Request
    class Standard < Solr::Request::Select
      def initialize(params)
        super('standard')
        
        raise ":query parameter required" unless params[:query]
        
        # devour StandardRequestHandler params
        @query = params.delete(:query)
        @sort = params.delete(:sort) # TODO add validation such that only :ascending and :descending are supported
        @default_field = params.delete(:default_field)
        @operator = params.delete(:operator)
        @operator = @operator == :and ? "AND" : "OR" if @operator # TODO add validation such that only :and or :or are supported

        # devour common parameters
        @start = params.delete(:start) # TODO validate integer
        @rows = params.delete(:rows)   # TODO validate integer
        @filter_queries = params.delete(:filter_queries)
        @field_list = params.delete(:field_list) || ["*","score"]
        @debug_query = params.delete(:debug_query)
        @explain_other = params.delete(:explain_other)
        
        # devour faceting parameters
        @facets = params.delete(:facets)
        
        #TODO model highlighting parameters: http://wiki.apache.org/solr/HighlightingParameters
        
        raise "Invalid parameters: #{params.keys.join(',')}" if params.size > 0
      end
      
      def to_hash
        hash = {}
        
        # standard request param processing
        sort = @sort.collect do |sort|
          key = sort.keys[0]
          "#{key.to_s} #{sort[key] == :descending ? 'desc' : 'asc'}"
        end.join(',') if @sort
        q = sort ? "#{@query};#{sort}" : @query
        hash[:q] = q
        hash[:"q.op"] = @operator
        hash[:df] = @default_field

        # common parameter processing
        hash[:start] = @start
        hash[:rows] = @rows
        hash[:fq] = @filter_queries
        hash[:fl] = @field_list.join(',')
        hash[:debugQuery] = @debug_query
        hash[:explainOther] = @explain_other
        
        # facet parameter processing
        if @facets
          hash[:facet] = true
          hash[:"facet.field"] = []
          hash[:"facet.query"] = @facets[:queries]
          hash[:"facet.missing"] = @facets[:missing]
          hash[:"facet.zeros"] = @facets[:zeros]
          hash[:"facet.limit"] = @facets[:limit]
          @facets[:fields].each do |f|
            if f.kind_of? Hash
              key = f.keys[0]
              value = f[key]
              hash[:"facet.field"] << key
              hash[:"f.#{key}.facet.missing"] = value[:missing]
              hash[:"f.#{key}.facet.zeros"] = value[:zeros]
              hash[:"f.#{key}.facet.limit"] = value[:limit]
            else
              hash[:"facet.field"] << f
            end
          end
        end
        
        hash.merge(super.to_hash)
      end

    end
  end
end
