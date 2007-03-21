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

module Flare
  module ActionControllerExtensions
    
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def flare(options={})
        include Flare::ActionControllerExtensions::InstanceMethods
      end
    end
    
    module InstanceMethods
      
      # Assumes @flare is set by a before_filter
      
      def index
        session[:page] = params[:page].to_i if params[:page]
        session[:page] = 1 if session[:page] <= 0

        @results_per_page = 25

        @start = (session[:page] - 1) * @results_per_page

        @response = @flare.search(@start, @results_per_page)
      end

      def facet
        puts "---- facet: #{params[:field]}"
        @facets = @flare.retrieve_field_facets(params[:field])
      end

      def auto_complete_for_search_query
        # TODO instead of "text", default to the default search field configured in schema.xml
        @values = @flare.retrieve_field_facets("text", 5, params['search']['query'].downcase)

        render :partial => 'suggest'
      end


      def add_query
        @flare.queries << {:query => params[:search][:query]}
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def update_query
        logger.debug "update_query: #{params.inspect}"
        @flare.queries[params[:index].to_i][:query] = params[:value]
        session[:page] = 1
        render :update do |page|
          page.redirect_to '/browse'
        end
      end

      def invert_query
        q = @flare.queries[params[:index].to_i]
        q[:negative] = !q[:negative]
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def remove_query
        @flare.queries.delete_at(params[:index].to_i)
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def invert_filter
        f = @flare.filters[params[:index].to_i]
        f[:negative] = !f[:negative]
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def remove_filter
        @flare.filters.delete_at(params[:index].to_i)
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def add_filter
        @flare.filters << {:field => params[:field], :value => params[:value], :negative => (params[:negative] ? true : false)} 
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def add_saved_search
        @flare.applied_facet_queries << {:name => params[:name], :negative => (params[:negative] ? true : false)}
        redirect_to :action => 'index'
      end

      def remove_saved_constraint
        @flare.applied_facet_queries.delete_at(params[:index].to_i)
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def clear
        @flare.clear
        redirect_to :action => 'index'
      end

      def show_saved
        query = @flare.facet_queries[params[:name]]
        @flare.applied_facet_queries << {:name => params[:name], :negative => (params[:negative] ? true : false)}
        index
        render :action => 'index'
      end

      def save
        @flare.facet_queries[params[:name]] = {:filters => @flare.filters.clone, :queries => @flare.queries.clone}
        redirect_to :action => 'index'
      end

      def remove_saved_search
        puts "---- BEFORE", @flare.to_s
        @flare.facet_queries.delete(params[:name])
        @flare.applied_facet_queries.delete_if {|f| params[:name] == f[:name]}
        puts "---- AFTER", @flare.to_s
        session[:page] = 1
        redirect_to :action => 'index'
      end

      def invert_saved_constraint
        f = @flare.applied_facet_queries[params[:index].to_i]
        f[:negative] = !f[:negative]
        session[:page] = 1
        redirect_to :action => 'index'
      end
    end
    
  end
end

module ActionController
  class Base
    include Flare::ActionControllerExtensions
  end
end