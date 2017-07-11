require 'csv'
require 'httparty'

csv_text = File.read('/Users/adamrenschen/Desktop/msbio_origins_etl/msb_origins_treatments_reformatted.csv')
csv = CSV.parse(csv_text, :headers => true)
csv.each do |row|
  HTTParty.post("https://uat.champagne.ucsf.edu/api/v1/treatments", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token="AUTH TOKEN GOES HERE"' })
end
