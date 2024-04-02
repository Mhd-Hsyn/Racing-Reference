run monogodb database:

docker run -d \
  --name my-mongodb \
  --restart always \
  -p 27017:27017 \
  -e MONGO_INITDB_DATABASE=neuro-gpt \
  -e DATASTORE_HOST=localhost \
  -e DATASTORE_PORT=27017 \
  -e DATASTORE_TIMEOUT=500 \
  mongo

volume mounting:

docker run -d \
  --name my-mongodb \
  --restart always \
  -p 27017:27017 \
  -v mongodb:/data/db \
  -e MONGO_INITDB_DATABASE=neuro-gpt \
  -e DATASTORE_HOST=localhost \
  -e DATASTORE_PORT=27017 \
  -e DATASTORE_TIMEOUT=500 \
  mongo



download compass(ubuntu):

wget https://downloads.mongodb.com/compass/mongodb-compass_1.28.1_amd64.deb

download(mac):

https://www.geeksforgeeks.org/how-to-install-mongodb-compass-on-macos/

install compass:

sudo apt install ./mongodb-compass_1.28.1_amd64.deb

Docker run:

 docker build -t racing-reference-backend:v1 . && docker run -p 9005:9005 racing-reference-backend:v1

 docker run -d -p 9005:9005 racing-reference-backend:v1
