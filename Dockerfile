FROM node:latest
WORKDIR /app
ENV HOST="192.168.65.2" USER="root" PASSWORD="" PORT=3306 DATABASE="supersample" 
ENV HOST2="172.17.0.2" USER2="postgres" PASSWORD2="mysecretpassword" PORT2="5432" DATABASE2="staging_ingestion"
COPY package.json /app
RUN npm install
COPY . /app
CMD node index.js