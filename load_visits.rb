require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "#{@working_dir_base}#{@working_dir}"
input_csv = "#{@csv_prefix}visits.csv"
log_filename = "visits_log_#{timestamp}.txt"
transaction_log = "visits_transaction_log_#{timestamp}.txt"

failed_ids = []
successful_ids = []

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

File.open(File.join(working_dir, transaction_log), 'w') { |file|
  csv.each do |row|

    if row["date"]
      @d = row["date"].split("\/")
      unless @d[2].match('/\d{4}/')
        @d[2] = "20#{@d[2]}"
      end
      unless @d[0].size == 2
        @d[0] = "0#{@d[0]}"
      end
      unless @d[1].size == 2
        @d[1] = "0#{@d[1]}"
      end
      row["date"] = Date.strptime(@d.join("-"), '%m-%d-%Y')
    end

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
    @response = HTTParty.post("#{@production_root_url}#{@visits_url}", :body => visit_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token=""'})
    file.write("RESPONSE:\n#{@response.headers["status"]}\n#{@response}\n-----------------------------------------\n")

    if @response.headers["status"] != "201 Created"
      failed_ids.push([row["external_identifier"], @response.headers["status"]])
    else
      successful_ids.push(row["external_identifier"])
    end
  end
}

File.open(File.join(working_dir, log_filename), 'w') { |file|
  file.write("Process started: #{timestamp}\n")
  file.write("Origins visits upload report\n")
  file.write("Successful uploads of visits: #{successful_ids.size}\n")
  file.write("Failed uploads of visits: #{failed_ids.size}\n")
  file.write("Failed ids:\n")
  failed_ids.each do |fid|
    file.write("#{fid}\n")
  end
}
