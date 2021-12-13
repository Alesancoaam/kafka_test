require( "dotenv" ).config();

const { 
    KAFKA_TOPIC: topic,
    KAFKA_GROUP_ID: groupId,
    KAFKA_AUTO_COMMIT_THRESHOLD: autoCommitThreshold, 
    KAFKA_PARTITIONS_CONSUMED_CONCURRENTLY: pcc
} = process.env

const { manageKafka } = require( "./services/kafka" )
const { orderHandler: handleEachMessage } = require( "./handlers/order" );

// Function for tests
const sleep = ( ms ) => new Promise( ( resolve ) => setTimeout( resolve, ms ) );

// Main function
const main = async() =>{

    const consumer = await manageKafka( { topic, groupId } ).catch( ( error ) =>{

        console.log( `[ERROR] main -> ${error?.message}` );

    } );

    await consumer.run( {
        autoCommitThreshold,
        partitionsConsumedConcurrently: parseInt( pcc ),
        eachMessage: handleEachMessage
    } );

}

main();