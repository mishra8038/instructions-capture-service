Dev Run Configuration Instructions
Please set profile as dev. 


Testing instructions:
Both the app and kafka container should be started from docker compose. 
app will listen on port 8080. 
kafka container is expected to be running on port 9092.
Postman collection is available for testing, the csv, json file locations will need to be updated.
Link: https://.postman.co/workspace/My-Workspace~19dbbec3-3d4b-464a-8257-332e5fbdeac5/collection/27016534-a05f3f87-809a-4b7e-bf77-406243e49855?action=share&creator=27016534&active-environment=27016534-d7e211b7-716a-4cfe-acad-a9ee1d21bdd2



Kafka Configuration Asumptions:
Kafka Stream is an experimental feature and is only enabled if the spring profile kstreams is selected. 
Dev profile by default only enables Kafka Listener. 
    
Production Deployment Notes
Produciton deployment docker compose file assumes that a production grade kafka broker will be available on port 9092.
