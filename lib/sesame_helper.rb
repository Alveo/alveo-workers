require 'rdf'
require 'json/ld'

module SesameHelper


  def create_rdf_graph(item_json_ld)
    json_ld = adapt_alveo_json_ld(item_json_ld)
    JSON::LD::API.toRdf(json_ld)
  end


  def adapt_alveo_json_ld(item_json_ld)
    json_ld = {}
    json_ld['@context'] = item_json_ld['@context']
    item_metadata = item_json_ld['alveo:metadata']
    # TODO: this fulltext field is only used by solr,
    # so maybe we should delete it in the upload worker
    # to reduce network traffic
    item_metadata.delete('alveo:full_text')

    # TODO: should remove dc:source property for
    # each document since it is a local path

    json_ld['@graph'] = [item_metadata]
    ['ausnc:document', '@graph'].each { |key|
      if item_json_ld.has_key? key
        json_ld['@graph'].concat(item_json_ld[key])
      end
    }
    json_ld
  end


end
