module SolrConstants

  @@RDF_RELATION_TO_FACET_MAP = {
    'http://purl.org/dc/terms/isPartOf' => 'collection_name_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/mode' => 'AUSNC_mode_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/speech_style' => 'AUSNC_speech_style_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/publication_status' => 'AUSNC_publication_status_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/written_mode' => 'AUSNC_written_mode_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/interactivity' => 'AUSNC_interactivity_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/communication_context' => 'AUSNC_communication_context_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/communication_medium' => 'AUSNC_communication_medium_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/communication_setting' => 'AUSNC_communication_setting_facet',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/audience' => 'AUSNC_audience_facet',
    'http://www.language-archives.org/OLAC/1.1/discourse_type' => 'OLAC_discourse_type_facet',
    'http://www.language-archives.org/OLAC/1.1/language' => 'OLAC_language_facet'
  }.freeze

  #dyamic fields solr prefix
  @@RDF_NS_TO_SOLR_PREFIX_MAP = {
    'http://ns.ausnc.org.au/schemas/ace/' => 'ACE_',
    'http://ns.ausnc.org.au/schemas/ausnc_md_model/' => 'AUSNC_',
    'http://ns.ausnc.org.au/schemas/austlit/' => 'AUSTLIT_',
    'http://ns.ausnc.org.au/schemas/cooee/' => 'COOEE_',
    'http://ns.ausnc.org.au/schemas/gcsause/' => 'GCSAUSE_',
    'http://ns.ausnc.org.au/schemas/ice/' => 'ICE_',
    'http://purl.org/dc/terms/' => 'DC_',
    'http://purl.org/dc/elements/1.1/' => 'DC_',
    'http://purl.org/ontology/bibo/' => 'PURL_BIBO_',
    'http://purl.org/vocab/bio/0.1/' => ' PURL_VOCAB_',
    'http://www.language-archives.org/OLAC/1.1/' => 'OLAC_',
    'http://xmlns.com/foaf/0.1/' => 'FOAF_',
    'http://www.loc.gov/loc.terms/relators/' => 'LoC_',
    'http://alveo.edu.au/vocabulary/' => 'AUSTALK_',
    'http://ns.austalk.edu.au/' => 'Alveo_',
    'http://hcsvlab.org/vocabulary/' => 'http_hcsvlab_org_vocabulary_',
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#' => 'RDF_',
    'http://www.w3.org/1999/02/22-rdf-syntax-ns/' => 'RDF_' #TODO: Hack
  }.freeze

  @@DOCUMENT_FIELD_TO_RDF_RELATION_MAP = {
    'DC_type_facet' => 'http://purl.org/dc/terms/type',
    'DC_extent_sim' => 'http://purl.org/dc/terms/extent',
    'DC_extent_tesim' => 'http://purl.org/dc/terms/extent'
  }.freeze

  @@MAPPED_FIELDS = {
    'default_data_owner'=> 'data_owner@intersect.org.au', ## TODO, this should be configurable
    'data_owner_field' => 'http://id.loc.gov/vocabulary/relators/rpy',
    'collection_field' => 'http://purl.org/dc/terms/isPartOf',
    'identifier_field' => '@id',
    'created_field' => 'http://purl.org/dc/terms/created',
    'indexable_document' => 'http://hcsvlab.org/vocabulary/indexable_document',
    'source' => 'http://purl.org/dc/terms/source'
  }.freeze

end