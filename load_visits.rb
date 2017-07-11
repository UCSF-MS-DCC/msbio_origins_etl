require 'csv'
require 'httparty'

csv_text = File.read('/Users/adamrenschen/Desktop/msbio_origins_etl/msb_origins_visits_reformatted.csv')
csv = CSV.parse(csv_text, :headers => true)
@failed_uploads = []
csv.each do |row|
  @res = HTTParty.post("https://uat.champagne.ucsf.edu/api/v1/visits", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token="AUTH TOKEN GOES HERE"' })
  if @res["status"] != "201 Created"
    @failed_uploads.push(row["external_identifier"])
  end
end
puts "#{@failed_uploads.size} failed uploads."
