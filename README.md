Some test of beam FileIO :
- fileio.FileioCountWords : count words from text files
- fileio.FileIOXML : retrieve names from XML file
- fileio.FileIOCsv2Avro : convert a csv file to Avro one

Some benchmark :
- Bench.ReadText : read a text file
- Bench.ReadXML : read a XML file
- Bench.WriteText : read a text file and write it to another place
- Bench.WriteXML : read a XML file and store it as csv in another place
- Bench.WriteCMLToAvro : read XML and store it as csv in another place

To execute POCs create an aws.conf file in the same directory of this README.md and set those properties:
```
access_key=xxxxxxxx
secret_key=xxxxxxxx
region=xxxxxxxxxxx
```
To run :
```
mvn compile exec:java -Dexec.mainClass=fileio.FileIOXML -Pdirect-runner
```
