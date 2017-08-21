$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'yaml'
require 'json'
require 'austalk_ingester'
require 'easy_logging'

include EasyLogging

@ingesting = true
@resume_point = -1
# TODO move these fields to a config file
@resume = 'austalk_resume.log'
@processed = 'austalk_processed.log'

def get_file_paths(directory)
  file_paths = Dir[File.join(directory, '*.json')]
  processed = []
  if File.exists? @processed
    processed = File.read(@processed).split
  end
  file_paths.select! { |file|
    !processed.include? file
  }
  if File.exists? @resume
    resume_data = File.read(@resume).split
    resume_file = resume_data.first
    file_paths.delete_if { |path|
      path = resume_file
    }
    @resume_point = resume_data.last.to_i
    file_paths.unshift(resume_file)
  end
  file_paths
end

def main(options, collection, directory)
  file_paths = get_file_paths(directory)
  ingester = AusTalkIngester.new(options)
  ingesting = true
  Signal.trap('TERM') {
    ingesting = false
    ingester.ingesting = ingesting
  }
  ingester.connect
  file_paths.each { |file_path|
    ingester.process_chunk(file_path, collection, @resume_point)
    @resume_point = -1
    if ingesting
      File.open(@processed, 'a') { |processed|
        processed.write("#{file_path}\n")
      }
    else
      File.open(@resume, 'w') { |processed|
        processed.write("#{file_path}\t#{ingester.record_count}\n")
      }
      break
    end
    while ingester.monitor_queues_message_count > 0
      sleep options[:monitor_poll]
    end
  }
  ingester.close
end


if __FILE__ == $PROGRAM_NAME
  Process.setproctitle('AusTalkIngester')
  Process.daemon(nochdir=true)
  config = YAML.load_file("#{File.dirname(__FILE__)}/../config.yml")
  options = config[:ingester]

  EasyLogging.log_destination = config[:common][:ingester_log]
  # usage ingest_austalk.rb <collection> <dir>
  # to ingest the named collection from the given directory full of .json files
  # if collection is ommitted, defaults to 'austalk'
  if ARGV.length == 1
      main(options, 'austalk', ARGV[0])
  elsif ARGV.length == 2
      main(options, ARGV[0], ARGV[1])
  else
      puts "Usage: ingest_austalk.rb <collection>? <dir>"
  end
end
