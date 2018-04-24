import avro.schema
import csv
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("user.avsc", 'r').read())

writer = DataFileWriter(open("resultFile.avro", "wb"), DatumWriter(), schema)

with open ('example.csv', 'rb') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    for row in reader:
        writer.append({"name": row['name'], "id": int(row["id"]), "domain": row['domain']})


writer.close()
reader = DataFileReader(open("resultFile.avro", "rb"), DatumReader())
for user in reader:
    print(user)

reader.close()