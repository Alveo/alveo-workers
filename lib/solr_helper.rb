module SolrHelper


  def remove_ns(value)

      if value.include?(":") and not value.start_with?('http')
          ns, value = value.split(':')
      end
      value
  end

  def create_solr_document(item_json_ld)

    # these map to NAMESPACE_name_facet (eg. OLAC_language_facet)
    facets = ['olac:discourse_type', 'olac:language', 'ausnc:mode', 'ausnc:speech_style',
              'ausnc:interactivity', 'ausnc:communication_context', 'ausnc:communication_setting',
              'ausnc:communication_medium', 'ausnc:audience', 'ausnc:written_mode',
              'ausnc:publication_status']

    excluded = ['@type', '@id', 'dc:isPartOf', 'dc:created', 'dc:title']

    item_metadata = item_json_ld['alveo:metadata']
    item_metadata.default = 'unspecified'

    result = {
      collection_name_facet: item_metadata['dc:isPartOf'],
      date_group_facet: item_json_ld['generated']['date_group'],
      DC_type_facet: item_json_ld['generated']['types'],
      handle: item_json_ld['generated']['handle'],
      id: item_json_ld['generated']['handle'],
      full_text: item_metadata['alveo:fulltext'], #TODO: fulltext should be a property of a document
      discover_access_group_ssim: "#{item_metadata['dc:isPartOf']}-discover",
      read_access_group_ssim: "#{item_metadata['dc:isPartOf']}-read",
      edit_access_group_ssim: "#{item_metadata['dc:isPartOf']}-edit",
      discover_access_person_ssim: item_json_ld['generated']['owner'],
      read_access_person_ssim: item_json_ld['generated']['owner'],
      edit_access_person_ssim: item_json_ld['generated']['owner'],
      DC_created_sim: item_metadata['dc:created'],
      DC_created_tesim: item_metadata['dc:created'],
      DC_title_sim: item_metadata['dc:title'],
      DC_title_tesim: item_metadata['dc:title']    }

    # map facets to given values, strip namespace from any value
    facets.each do |rdfname|
        ns, name = rdfname.split(':')
        key = :"#{ns.upcase}_#{name}_facet"
        result[key] = remove_ns(item_metadata[rdfname])
    end

    # map all other field names to NS_name_sim and NS_name_tesim
    # (_sim and _tesim suffixes seem to be related to hydra but not sure)
    item_metadata.each do |key, value|
        unless facets.include? key or excluded.include? key
            ns, name = key.split(':')
            key = "#{ns.upcase}_#{name}"
            value = remove_ns(value)
            result[:"#{key}_sim"] = value
            result[:"#{key}_tesim"] = value
        end
    end
    result
  end

end
