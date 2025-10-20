Alan Castillo

1. Since the log data is expected to be much larger than the hostname_country list, 
I implemented these tasks using map-side joins. For Task1, the mapper emits <country, 1> for each
log entry in the data by joining on the hostname_country data using the IP Address as the foreign key. 
The reducer then sums the count of each country and returns <country, sum>. This is then passed to the 
Task1Sort Job that flips the key value pairs and sorts them in descending order.

2. In the mapper, using the Map-side reduce, form a composite key of the form (country,url) and keep a keep a count
of each instance in the log. The reducer will sum each composite key and will emit <(country, URL), count>. 

3. The Mapper will emit <URL, Country> values, and the reducer will filter through the countries 
creating a list of unique countries. The reducer will emit final output <URL, listOfCountries>
