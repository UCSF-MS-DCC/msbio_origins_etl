require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%b_%d_%Y')
working_dir = "/Users/adamrenschen/Desktop/msbio_origins_etl/source_csv_08022017"
input_csv = "visits.csv"
log_filename = "visits_log_#{timestamp}.txt"

failed_ids = []
successful_ids = []

@emr = {"2101" => "52537", "2102" => "899097", "2103" => "898422", "2104" => "924920", "2105" => "46700", "2106" => "898562", "2107" => "962600", "2110" => "780076" } #some origins are in the database as EMR patients. This hash maps epic id number with msbio database external identifiers.

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

csv.each do |row|
  if row["external_identifier"]
    row["source_id"] = 2 # regardless of EMR or origins patient source, putting all visits in as EPIC
    row["external_identifier"] = "2000" + row["external_identifier"].to_s #visit id. modifying from the origins database id to avoid conflicts with epic visit ids
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

    if @emr[row["patient_external_identifier"].to_s]
      row["patient_external_identifier"] = @emr[row["patient_external_identifier"].to_s] #patient id. Some origins patients are in the database as EMR with ids that don't follow the convention for most of the origins patients. See hash above.
      row["patient_source_id"] = 1 # EMR source id number.
    else
      old_id = row["patient_external_identifier"].to_s
      row["patient_external_identifier"] = "2000" + old_id #patient id. The naming convention for this field (excepting origins-EMR patients) is 2000 + EPIC id number (ie EPIC2199 would be 20002199)
      row["patient_source_id"] = 2 # EPIC source id number.
    end

    @response = HTTParty.post("#{@production_root_url}#{@visits_url}", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token="c5809928-ebe8-4b7a-ae74-40e59b61f47b"'})

    if @response.headers["status"] != "201 Created"
      failed_ids.push([row["external_identifier"], @response.headers["status"]])
    else
      successful_ids.push(row["external_identifier"])
    end
  end
end

File.open(File.join(working_dir, log_filename), 'w') { |file|
  file.write("#{timestamp}\n")
  file.write("Origins visits upload report\n")
  file.write("Successful uploads of visits: #{successful_ids.size}\n")
  file.write("Failed uploads of visits: #{failed_ids.size}\n")
  file.write("Failed ids:\n")
  failed_ids.each do |fid|
    file.write("#{fid}\n")
  end
}
