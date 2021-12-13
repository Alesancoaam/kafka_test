require( "dotenv" ).config();

const { KAFKA_TOPIC: topic, KAFKA_NUMBER_PARTITION: partitions } = process.env

const { getOrder } = require( "./models/order" );
const { manageKafka } = require( "./services/kafka" );

// Function for tests
const sleep = ( ms ) => new Promise( ( resolve ) => setTimeout( resolve, ms ) );

// Process order and send to kafka
const processOrders = async ( { producer } ) => {

    try {

        const orders = await getOrder();

        console.log( `Will process [${orders?.length}] orders` );

        for( const order of orders ){

            await producer.send( {
                topic,
                messages: [ { value: JSON.stringify( order ) } ]
            } );

        }

        console.log( `Successfully processed [${orders?.length}] orders` );

    } catch ( error ) {

        console.log( `[ERROR] processOrders -> ${error?.message}` );

    }

}

// Main function
const main = async () => {

    const producer = await manageKafka( { topic, partitions } ).catch( ( error ) =>{

        console.log( `[ERROR] main -> ${error?.message}` );

    } );

    while ( true ) {

        await processOrders( { producer } );

    }

}

main();