const { Kafka } = require( "kafkajs" );
const { 
    KAFKA_CLIENT_ID: clientId,
    KAFKA_BROKERS: brokers,
} = process.env

const kafka = new Kafka( {  
    clientId,
    brokers: brokers.split( "," ),
    retry: {
        initialRetryTime: 300,
        retries: 10
    }
} );

module.exports = {
    kafka
}