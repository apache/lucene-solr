class Solr::Request::Dismax < Solr::Request::Standard

  VALID_PARAMS.replace(VALID_PARAMS + [:tie_breaker, :query_fields, :minimum_match, :phrase_fields, :phrase_slop,
                                       :boost_query, :boost_functions])

  def initialize(params)
    super(params)
    @query_type = "dismax"
  end
  
  def to_hash
    hash = super
    hash[:tie] = @params[:tie_breaker]
    hash[:mm]  = @params[:minimum_match]
    hash[:qf]  = @params[:query_fields]
    hash[:pf]  = @params[:phrase_fields]
    hash[:ps]  = @params[:phrase_slop]
    hash[:bq]  = @params[:boost_query]
    hash[:bf]  = @params[:boost_functions]
    return hash
  end

end