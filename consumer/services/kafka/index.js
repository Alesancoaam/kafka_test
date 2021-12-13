const { kafka } = require( "../../config/kafka" );

const manageKafka = async( { topic, groupId } ) =>{

    try{

        console.log( `[KAFKA] consumer will subscribe topic [${topic}]` );

        const consumer = kafka.consumer( { groupId } );
        await consumer.connect();
        await consumer.subscribe( { topic, fromBeginning: true } );

        console.log( `[KAFKA] consumer successfully subscribed for topic [${topic}]` );

        return consumer;

    } catch( error ){
        
        throw new Error( `manageKafka -> ${error?.message}` );

    }

}

module.exports = {
    manageKafka
}