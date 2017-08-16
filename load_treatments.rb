require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "#{@working_dir_base}#{@working_dir}"
input_csv = "#{@csv_prefix}treatments.csv"
log_filename = "treatments_log_#{timestamp}.txt"

failed_ids = []
successful_ids = []

@emr = {"2101" => "52537", "2102" => "899097", "2103" => "898422", "2104" => "924920", "2105" => "46700", "2106" => "898562", "2107" => "962600", "2110" => "780076" } #some origins are in the database as EMR patients. This hash maps epic id number with msbio database external identifiers.

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

csv.each do |row|
  row["source_id"] = 6 # regardless of EMR or origins patient source, putting all treatments in as origins
  row["patient_source_id"] = 6
  # row["external_identifier"] = "2000" + row["external_identifier"] #treatment id. modifying from the origins database id to avoid conflicts with epic attack ids

  #in msbase db, some treatments only have month/year for start or end dates. The following code checks for a year-month-day format, and adjusts the date so that treatments start on the first of a month and end on the last day of the month (eom = end of month) if there is no day specified
  unless row["start_date"].match(/\d{4}\-\d{2}\-\d{2}/)
    arr = row["start_date"].split("\/")
    if arr[0].size < 2
      arr[0] = "0#{arr[0]}"
    end
    row["start_date"] = "#{arr[1]}-#{arr[0]}-01"
  end
  unless row["end_date"].match(/\d{4}\-\d{2}\-\d{2}/)
    arr = row["end_date"].split("\/")
    if arr[0].size < 2
      arr[0] = "0#{arr[0]}"
    end
    eom = {"01"=>"31", "02"=>"28", "03"=>"31", "04"=>"30", "05"=>"31", "06"=>"30", "07"=>"31", "08"=>"31", "09"=>"30", "10"=>"31", "11"=>"30", "12"=>"31"}
    row["end_date"] = "#{arr[1]}-#{arr[0]}-#{eom[arr[0]]}"
  end
  if @emr[row["patient_external_identifier"]]
    row["patient_external_identifier"] = @emr[row["patient_external_identifier"]] #patient id. Some origins patients are in the database as EMR with ids that don't follow the patient_external_identifier convention for origins patients. See hash above.
    row["patient_source_id"] = 1 # EMR source id number.
  # else
  #   old_id = row["patient_external_identifier"]
  #   row["patient_external_identifier"] = "2000" + old_id #patient id. The naming convention for this field (excepting origins-EMR patients) is 2000 + EPIC id number (ie EPIC2199 would be 20002199)
  #   row["patient_source_id"] = 2 # EPIC source id number.
  end

  @response = HTTParty.post("#{@production_root_url}#{@treatments_url}", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token="c5809928-ebe8-4b7a-ae74-40e59b61f47b"'})

  if @response.headers["status"] != "201 Created"
    failed_ids.push([row["external_identifier"], @response.headers["status"]])
  else
    successful_ids.push(row["external_identifier"])
  end
end

File.open(File.join(working_dir, log_filename), 'w') { |file|
  file.write("Process started: #{timestamp}\n")
  file.write("Origins treatments upload report\n")
  file.write("Successful uploads of treatments: #{successful_ids.size}\n")
  file.write("Failed uploads of treatments: #{failed_ids.size}\n")
  file.write("Failed ids:\n")
  failed_ids.each do |fid|
    file.write("#{fid}\n")
  end
}
