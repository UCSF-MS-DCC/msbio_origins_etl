#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "/var/lib/mysql-files"
input_csv = "origins_msbio_etl_visits.csv"
log_directory = "/home/arenschen/origins_msbio_pipeline/logs"
transaction_log = "create_new_visits_transaction_log_#{timestamp}.txt"

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

File.open(File.join(log_directory, transaction_log),'w') do |file|
  csv.each do |row|
#    if row["date"]
#      @d = row["date"].split("\/")
#      unless @d[2].match('/\d{4}/')
#        @d[2] = "20#{@d[2]}"
#      end
#      unless @d[0].size == 2
#        @d[0] = "0#{@d[0]}"
#      end
#      unless @d[1].size == 2
#        @d[1] = "0#{@d[1]}"
#      end
#      row["date"] = Date.strptime(@d.join("-"), '%m-%d-%Y')
#    end

    visit_hash = {"external_identifier" => row["external_identifier"],
                  "patient_external_identifier" => row["patient_external_identifier"],
                  "patient_source_id" => 6,
                  "source_id" => 6,
                  "date" => row["date"],
                  "age_at_visit" => row["age_at_visit"],
                  "msss" => row["msss"],
                  "edss" => row["edss"],
                  "fssc_visual" => row["fssc_visual"],
                  "fssc_brainstem" => row["fssc_brainstem"],
                  "fssc_pyramidal" => row["fssc_pyramidal"],
                  "fssc_cerebellar" => row["fssc_cerebellar"],
                  "fssc_sensory" => row["fssc_sensory"],
                  "fssc_bowel" => row["fssc_bowel"],
                  "fssc_mental" => row["fssc_mental"],
                  "pasat_score" => row["pasat_score"],
                  "nhpt_dominant_time" => row["nhpt_dominant_time"],
                  "nhpt_nondominant_time" => row["nhpt_nondominant_time"],
                  "timed_walk_trial1_time" => row["timed_walk_trial1_time"],
                  "timed_walk_trial2_time" => row["timed_walk_trial2_time"],
                  "disease_course" => row["disease_course"],
                  "disease_duration" => row["disease_duration"]
                }
    file.write("POST:\n #{visit_hash.to_json}\n")
    @response = HTTParty.post("#{@production_root_url}#{@visits_url}", :body => visit_hash.to_json, :headers => { 'Content-type'=>'application/json', 'Authorization'=>'Token token=""'})
    file.write("RESPONSE:\n#{@response.headers["status"]}\n#{@response}\n------------------------------------------------------------------\n")
    unless @response.headers["status"] == "201 Created"
        file.write("PUT: #{row.to_hash.to_json}\n")
        @put_response = HTTParty.put("#{@production_root_url}#{@visits_put_url}#{row["external_identifier"]}", :body => row.to_hash.to_json, :headers => { "Content-type"=>"application/json", "Authorization"=>'Token token=""'})
        file.write("RESPONSE:\n#{@put_response.headers["status"]}\n#{@put_response}\n-------------------------------------------------------------------\n")
    end
  end
end

