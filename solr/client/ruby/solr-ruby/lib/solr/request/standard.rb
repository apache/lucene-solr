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

class Solr::Request::Standard < Solr::Request::Select

  VALID_PARAMS = [:query, :sort, :default_field, :operator, :start, :rows, :shards,
    :filter_queries, :field_list, :debug_query, :explain_other, :facets, :highlighting, :mlt]
  
  def initialize(params)
    super('standard')
    
    raise "Invalid parameters: #{(params.keys - VALID_PARAMS).join(',')}" unless 
      (params.keys - VALID_PARAMS).empty?
    
    raise ":query parameter required" unless params[:query]
    
    @params = params.dup
    
    # Validate operator
    if params[:operator]
      raise "Only :and/:or operators allowed" unless 
        [:and, :or].include?(params[:operator])
        
      @params[:operator] = params[:operator].to_s.upcase
    end

    # Validate start, rows can be transformed to ints
    @params[:start] = params[:start].to_i if params[:start]
    @params[:rows] = params[:rows].to_i if params[:rows]
    
    @params[:field_list] ||= ["*","score"]
    
    @params[:shards] ||= []
  end
  
  def to_hash
    hash = {}
    
    # standard request param processing
    hash[:sort] = @params[:sort].collect do |sort|
      key = sort.keys[0]
      "#{key.to_s} #{sort[key] == :descending ? 'desc' : 'asc'}"
    end.join(',') if @params[:sort]
    hash[:q] = @params[:query]
    hash["q.op"] = @params[:operator]
    hash[:df] = @params[:default_field]

    # common parameter processing
    hash[:start] = @params[:start]
    hash[:rows] = @params[:rows]
    hash[:fq] = @params[:filter_queries]
    hash[:fl] = @params[:field_list].join(',')
    hash[:debugQuery] = @params[:debug_query]
    hash[:explainOther] = @params[:explain_other]
    hash[:shards] = @params[:shards].join(',') unless @params[:shards].empty?
    
    # facet parameter processing
    if @params[:facets]
      # TODO need validation of all that is under the :facets Hash too
      hash[:facet] = true
      hash["facet.field"] = []
      hash["facet.query"] = @params[:facets][:queries]
      hash["facet.sort"] = (@params[:facets][:sort] == :count) if @params[:facets][:sort]
      hash["facet.limit"] = @params[:facets][:limit]
      hash["facet.missing"] = @params[:facets][:missing]
      hash["facet.mincount"] = @params[:facets][:mincount]
      hash["facet.prefix"] = @params[:facets][:prefix]
      hash["facet.offset"] = @params[:facets][:offset]
      hash["facet.method"] = @params[:facets][:method] if @params[:facets][:method]
      if @params[:facets][:fields]  # facet fields are optional (could be facet.query only)
        @params[:facets][:fields].each do |f|
          if f.kind_of? Hash
            key = f.keys[0]
            value = f[key]
            hash["facet.field"] << key
            hash["f.#{key}.facet.sort"] = (value[:sort] == :count) if value[:sort]
            hash["f.#{key}.facet.limit"] = value[:limit]
            hash["f.#{key}.facet.missing"] = value[:missing]
            hash["f.#{key}.facet.mincount"] = value[:mincount]
            hash["f.#{key}.facet.prefix"] = value[:prefix]
            hash["f.#{key}.facet.offset"] = value[:offset]
          else
            hash["facet.field"] << f
          end
        end
      end
    end
    
    # highlighting parameter processing - http://wiki.apache.org/solr/HighlightingParameters
    if @params[:highlighting]
      hash[:hl] = true
      hash["hl.fl"] = @params[:highlighting][:field_list].join(',') if @params[:highlighting][:field_list]

      snippets = @params[:highlighting][:max_snippets]
      if snippets
        if snippets.kind_of? Hash
          if snippets[:default]
            hash["hl.snippets"] = snippets[:default]
          end
          if snippets[:fields]
            snippets[:fields].each do |k,v|
              hash["f.#{k}.hl.snippets"] = v
            end
          end
        else
          hash["hl.snippets"] = snippets
        end
      end

      fragsize = @params[:highlighting][:fragment_size]
      if fragsize
        if fragsize.kind_of? Hash
          if fragsize[:default]
            hash["hl.fragsize"] = fragsize[:default]
          end
          if fragsize[:fields]
            fragsize[:fields].each do |k,v|
              hash["f.#{k}.hl.fragsize"] = v
            end
          end
        else
          hash["hl.fragsize"] = fragsize
        end
      end

      rfm = @params[:highlighting][:require_field_match]
      if nil != rfm
        if rfm.kind_of? Hash
          if nil != rfm[:default]
            hash["hl.requireFieldMatch"] = rfm[:default]
          end
          if rfm[:fields]
            rfm[:fields].each do |k,v|
              hash["f.#{k}.hl.requireFieldMatch"] = v
            end
          end
        else
          hash["hl.requireFieldMatch"] = rfm
        end
      end

      mac = @params[:highlighting][:max_analyzed_chars]
      if mac
        if mac.kind_of? Hash
          if mac[:default]
            hash["hl.maxAnalyzedChars"] = mac[:default]
          end
          if mac[:fields]
            mac[:fields].each do |k,v|
              hash["f.#{k}.hl.maxAnalyzedChars"] = v
            end
          end
        else
          hash["hl.maxAnalyzedChars"] = mac
        end
      end

      prefix = @params[:highlighting][:prefix]
      if prefix
        if prefix.kind_of? Hash
          if prefix[:default]
            hash["hl.simple.pre"] = prefix[:default]
          end
          if prefix[:fields]
            prefix[:fields].each do |k,v|
              hash["f.#{k}.hl.simple.pre"] = v
            end
          end
        else
          hash["hl.simple.pre"] = prefix
        end
      end

      suffix = @params[:highlighting][:suffix]
      if suffix
        if suffix.kind_of? Hash
          if suffix[:default]
            hash["hl.simple.post"] = suffix[:default]
          end
          if suffix[:fields]
            suffix[:fields].each do |k,v|
              hash["f.#{k}.hl.simple.post"] = v
            end
          end
        else
          hash["hl.simple.post"] = suffix
        end
      end

      formatter = @params[:highlighting][:formatter]
      if formatter
        if formatter.kind_of? Hash
          if formatter[:default]
            hash["hl.formatter"] = formatter[:default]
          end
          if formatter[:fields]
            formatter[:fields].each do |k,v|
              hash["f.#{k}.hl.formatter"] = v
            end
          end
        else
          hash["hl.formatter"] = formatter
        end
      end

      fragmenter = @params[:highlighting][:fragmenter]
      if fragmenter
        if fragmenter.kind_of? Hash
          if fragmenter[:default]
            hash["hl.fragmenter"] = fragmenter[:default]
          end
          if fragmenter[:fields]
            fragmenter[:fields].each do |k,v|
              hash["f.#{k}.hl.fragmenter"] = v
            end
          end
        else
          hash["hl.fragmenter"] = fragmenter
        end
      end

      merge_contiguous = @params[:highlighting][:merge_contiguous]
      if nil != merge_contiguous
        if merge_contiguous.kind_of? Hash
          if nil != merge_contiguous[:default]
            hash["hl.mergeContiguous"] = merge_contiguous[:default]
          end
          if merge_contiguous[:fields]
            merge_contiguous[:fields].each do |k,v|
              hash["f.#{k}.hl.mergeContiguous"] = v
            end
          end
        else
          hash["hl.mergeContiguous"] = merge_contiguous
        end
      end

      increment = @params[:highlighting][:increment]
      if increment
        if increment.kind_of? Hash
          if increment[:default]
            hash["hl.increment"] = increment[:default]
          end
          if increment[:fields]
            increment[:fields].each do |k,v|
              hash["f.#{k}.hl.increment"] = v
            end
          end
        else
          hash["hl.increment"] = increment
        end
      end

      # support "old style"
      alternate_fields = @params[:highlighting][:alternate_fields]
      if alternate_fields
        alternate_fields.each do |f,v|
          hash["f.#{f}.hl.alternateField"] = v
        end
      end

      alternate_field = @params[:highlighting][:alternate_field]
      if alternate_field
        if alternate_field.kind_of? Hash
          if alternate_field[:default]
            hash["hl.alternateField"] = alternate_field[:default]
          end
          if alternate_field[:fields]
            alternate_field[:fields].each do |k,v|
              hash["f.#{k}.hl.alternateField"] = v
            end
          end
        else
          hash["hl.alternateField"] = alternate_field
        end
      end

      mafl = @params[:highlighting][:max_alternate_field_length]
      if mafl
        if mafl.kind_of? Hash
          if mafl[:default]
            hash["hl.maxAlternateFieldLength"] = mafl[:default]
          end
          if mafl[:fields]
            mafl[:fields].each do |k,v|
              hash["f.#{k}.hl.maxAlternateFieldLength"] = v
            end
          else
            # support "old style"
            mafl.each do |k,v|
              hash["f.#{k}.hl.maxAlternateFieldLength"] = v
            end
          end
        else
          hash["hl.maxAlternateFieldLength"] = mafl
        end
      end

      hash["hl.usePhraseHighlighter"] = @params[:highlighting][:use_phrase_highlighter]

      hash["hl.useFastVectorHighlighter"] = @params[:highlighting][:use_fast_vector_highlighter]

      regex = @params[:highlighting][:regex]
      if regex
        if regex[:slop]
          if regex[:slop].kind_of? Hash
            if regex[:slop][:default]
              hash["hl.regex.slop"] = regex[:slop][:default]
            end
            if regex[:slop][:fields]
              regex[:slop][:fields].each do |k,v|
                hash["f.#{k}.hl.regex.slop"] = v
              end
            end
          else
            hash["hl.regex.slop"] = regex[:slop]
          end
        end
        if regex[:pattern]
          if regex[:pattern].kind_of? Hash
            if regex[:pattern][:default]
              hash["hl.regex.pattern"] = regex[:pattern][:default]
            end
            if regex[:pattern][:fields]
              regex[:pattern][:fields].each do |k,v|
                hash["f.#{k}.hl.regex.pattern"] = v
              end
            end
          else
            hash["hl.regex.pattern"] = regex[:pattern]
          end
        end
        if regex[:max_analyzed_chars]
          if regex[:max_analyzed_chars].kind_of? Hash
            if regex[:max_analyzed_chars][:default]
              hash["hl.regex.maxAnalyzedChars"] = regex[:max_analyzed_chars][:default]
            end
            if regex[:max_analyzed_chars][:fields]
              regex[:max_analyzed_chars][:fields].each do |k,v|
                hash["f.#{k}.hl.regex.maxAnalyzedChars"] = v
              end
            end
          else
            hash["hl.regex.maxAnalyzedChars"] = regex[:max_analyzed_chars]
          end
        end
      end

    end
    
    if @params[:mlt]
      hash[:mlt] = true
      hash["mlt.count"] = @params[:mlt][:count]
      hash["mlt.fl"] = @params[:mlt][:field_list].join(',')
      hash["mlt.mintf"] = @params[:mlt][:min_term_freq]
      hash["mlt.mindf"] = @params[:mlt][:min_doc_freq]
      hash["mlt.minwl"] = @params[:mlt][:min_word_length]
      hash["mlt.maxwl"] = @params[:mlt][:max_word_length]
      hash["mlt.maxqt"] = @params[:mlt][:max_query_terms]
      hash["mlt.maxntp"] = @params[:mlt][:max_tokens_parsed]
      hash["mlt.boost"] = @params[:mlt][:boost]
    end
    
    hash.merge(super.to_hash)
  end

end
