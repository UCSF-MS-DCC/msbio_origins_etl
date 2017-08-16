require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "#{@working_dir_base}#{@working_dir}"
input_csv = "#{@csv_prefix}patients.csv"
log_filename = "patients_log_#{timestamp}.txt"

failed_ids = []
successful_ids = []

@emr = {"2101" => "52537", "2102" => "899097", "2103" => "898422", "2104" => "924920", "2105" => "46700", "2106" => "898562", "2107" => "962600", "2110" => "780076" }

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

csv.each do |row|

  if @emr[row["external_identifier"]]
    row["external_identifier"] = @emr[row["external_identifier"]]
    row["source_id"] = 1
  else
    row["source_id"] = 6
  end
  # if row["dob"]
  #   # puts "#{row["dob"]}, #{row["dob"].class}"
  #   # row["dob"] = Date.strptime(row["dob"].to_s, '%m/%d/%y')
  # end

  @response = HTTParty.post("#{@production_root_url}#{@patients_url}", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token=""'})

  if @response.headers["status"] != "201 Created"
    failed_ids.push([row["external_identifier"], @response.headers["status"]])
  else
    successful_ids.push(row["external_identifier"])
  end
end


File.open(File.join(working_dir, log_filename), 'w') { |file|
  file.write("Process started: #{timestamp}\n")
  file.write("Origins patients upload report\n")
  file.write("Successful uploads of patients: #{successful_ids.size}\n")
  file.write("Failed uploads of patients: #{failed_ids.size}\n")
  file.write("Failed ids:\n")
  failed_ids.each do |fid|
    file.write("#{fid}\n")
  end
}
