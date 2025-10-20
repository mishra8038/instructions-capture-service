Dev Run Configuration Instructions
Please set profile as dev. 


Testing instructions: 
Kafka Configuration Asumptions:
Kafka Stream is an experimental feature and is only enabled if the spring profile kstreams is selected. 
Dev profile by default only enables Kafka Listener. 
    
Production Deployment Notes
Produciton deployment docker compose file assumes that a production grade kafka broker will be available on port 9092.
