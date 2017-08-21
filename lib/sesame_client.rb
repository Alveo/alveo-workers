require 'net/http/persistent'
require 'json'
require_relative 'net_http_overrides'
require_relative 'persistent_client'
require 'easy_logging'

class SesameClient < PersistentClient

  include EasyLogging

  @@PATHS = {}

  def initialize(config)
    logger.debug "#initialize"
    @base_url = config[:base_url]
    @paths = config[:paths]
    @config = config
    @mime_types = {sparql_json: 'application/sparql-results+json',
                   trig: 'application/x-trig',
                   turtle: 'text/turtle',
                   n3: 'text/rdf+n3'
                  }
    super('SesameClient')
  end

  def create_repository(name)
    logger.debug "#create_repository: name[#{name}]"

    existing_repositories = repositories
    if existing_repositories.include? name
      msg = "Repository already contains a collection named #{name}"
      logger.error "create_repository: #{msg}"

      raise msg
    end
    uri = get_statements_uri('SYSTEM')
    body = get_repository_template(name)
    request(uri, :post, {'Content-Type' => @mime_types[:trig]}, body)
    name
  end

  # TODO: move to module
  def get_repository_template(name)
    %Q(
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
        @prefix rep: <http://www.openrdf.org/config/repository#>.
        @prefix sr: <http://www.openrdf.org/config/repository/sail#>.
        @prefix sail: <http://www.openrdf.org/config/sail#>.
        @prefix sys: <http://www.openrdf.org/config/repository#> .
        @prefix ns: <http://www.openrdf.org/config/sail/native#>.

        _:#{name}{
          [] a rep:Repository ;
            rep:repositoryID "#{name}" ;
            rdfs:label "Metadata and Annotations for #{name} collection" ;
            rep:repositoryImpl [
              rep:repositoryType "openrdf:SailRepository" ;
              sr:sailImpl [
                sail:sailType "openrdf:NativeStore" ;
                ns:tripleIndexes "spoc,posc"
              ]
            ].
        }
        {
          _:#{name} a sys:RepositoryContext .
        }
      )
  end

  def insert_statements(repository, ttl_string)
    logger.debug "#insert_statements: repository[#{repository}], ttl_string[#{ttl_string}]"
    uri = get_statements_uri(repository)
    request(uri, :post, {'Content-Type' => @mime_types[:turtle]}, ttl_string)
  end

  def batch_insert_statements(repository, n3_string)
    logger.debug "#batch_insert_statements: repository[#{repository}], n3_string[#{n3_string.length}]"

    uri = get_statements_uri(repository)
    request(uri, :post, {'Content-Type' => @mime_types[:n3]}, n3_string)
  end

  def clear_repository(repository)
    logger.debug "#clear_repository: repository[#{repository}]"

    uri = get_statements_uri(repository)
    request(uri, :delete)
  end

  def repositories
    logger.debug "#repositories"

    uri = get_repositories_uri
    repositories = []
    query_results = parse_json_response(request(uri, :get, {'Accept' => @mime_types[:sparql_json]}))
    query_results['results']['bindings'].each { |repository|
      repository_name = repository['id']['value']
      repositories << repository_name unless repository_name == 'SYSTEM'
    }
    repositories
  end

  def get_repositories_uri
    logger.debug "#get_repositories_uri"

    URI.join(@base_url, 'repositories')
  end

  def get_statements_uri(repository)
    logger.debug "#get_statements_uri: repository[#{repository}]"

    statements_path = "repositories/#{repository}/statements"
    rlt = URI.join(@base_url, statements_path)
    logger.debug "get_statements_uri: @base_url[#{@base_url}], statements_path[#{statements_path}], rlt[#{rlt}]"

    rlt
  end

end