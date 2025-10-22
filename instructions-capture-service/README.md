

Introduction: 
Thank you for the assignment, it was a pleasure to work on this.
The application is a simple microservice that captures the instructions from a rest end point, in the form of a CSV (or json) file or simple application/json text body.
The application is built using Spring Boot and Spring Cloud. 
A docker compose file is included to run the application with a kafka container.
While the solution currently send back the procssed message in the HTTP response for testing purposes, in future, for scalability and throughput purposes, perhaps an HTTP response with 200 OK would suffice. 
Assuming this is the future ask, we will redirect all instructions recieved from the webservice to the inbound topic and allow asynchronous processing through the listender, avoiding sending out a bulky response. 

Seperate spring profiles have been included for the two solutions: 
Kafka Listener - Under the dev profile, the application listens on 
Kafka Streams - Under a seperate spring profile - kstreams, an experimental kafka-streams solution, with a concurrent hashmap to deduplicate the instructions, the hashmap uses HMAC algorithm to generate a unique key for each instructions, based on the business fields. 


Development Notes:
Dev Run Configuration Instructions
Please set profile as dev, or kstreams depending upon which solution is being tested / developed. 

Testing notes:
Both the app and kafka container should be started from docker compose.
app will listen on port 8080. 
kafka container is expected to be running on port 9092.
Postman collection is available for testing, the csv, json file locations will need to be updated.
Link: https://.postman.co/workspace/My-Workspace~19dbbec3-3d4b-464a-8257-332e5fbdeac5/collection/27016534-a05f3f87-809a-4b7e-bf77-406243e49855?action=share&creator=27016534&active-environment=27016534-d7e211b7-716a-4cfe-acad-a9ee1d21bdd2
Recommended profile for QA testing the application is dev.
A production profile has been included for discussion purposes. 


Unit Testing and End to End Testing: 
Since a part of the application has been generated through AI - a minimal set of unit tests and integration tests have been eclosed, that can provide assurance that the application code is working as expected.
End to End testing used TestContainers to spin up a kafka container listening on random port and tests the application flows for both the Kafka Listender solution and the Kafka Streams solution.


Kafka Streams
Kafka Configuration Asumptions:
Kafka Stream is an experimental feature and is only enabled if the spring profile kstreams is selected. 
Dev profile by default only enables Kafka Listener. 
    
Production Deployment Notes
Produciton deployment docker compose file assumes that a production grade kafka broker will be available on port 9092.
Logging has been truncated to INFO. 
Secrets configuration will be deffered based on inhouse devops guidance. 


