const { kafka } = require( "../../config/kafka" );

const connect = async() =>{

    const admin = kafka.admin();
    await admin.connect();
    return admin;

}

// Manage kafka flow
const manageKafka = async( { topic, partitions } ) =>{

    try{

        if( !topic ) throw new Error( `missing topic param` );
        if( !partitions ) throw new Error( `missing partitions param` );
        
        const producer = kafka.producer();
        await producer.connect();

        await manageTopics( { topic, partitions } );
        await managePartitions( { topic, partitions } )

        return producer

    } catch( error ){

        throw new Error( `manageKafka -> ${error?.message}` );

    }


}

// Manage kafka topics
const manageTopics = async( { topic, partitions } ) =>{

    let admin = null;
    let topicsList = [];

    try{

        admin = await connect();

        console.log( `[KAFKA] starting to manage topic [${topic}]` );

        topicsList = await admin.listTopics();

        // Check if topic is already created
        if( topicsList.includes( topic ) ){

            console.log( `[KAFKA] topic [${topic}] already created` );

        } else{

            console.log( `[KAFKA] creating topic [${topic}]` );

            const topicCreated = await admin.createTopics( {
                topics: [ {
                    topic,
                    numPartitions: partitions
                } ]
            } );

            if( !topicCreated ){

                console.log( `[KAFKA] error when creating topic [${topic}]` );
                throw new Error( `unable to create topic ${topic}` )

            }

            console.log( `[KAFKA] topic [${topic}] created` );

        }

    } catch( error ){

        throw new Error( `manageTopics -> ${error?.message}` );

    } finally{

        await admin?.disconnect();

    }

};

// Manage kafka partitions
const managePartitions = async( { topic, partitions } ) =>{

    let admin = null;
    try{

        admin = await connect();

        console.log( `[KAFKA] starting to manage topic [${topic}] with partitions [${partitions}]` );

        const { topics } = await admin.fetchTopicMetadata( { topics: [ topic ] } );

        // Create partitions if need
        if( parseInt( partitions ) > topics[ 0 ].partitions?.length ){

            await admin.createPartitions( { topicPartitions: [ { topic, count: partitions } ] } );

            const { topics } = await admin.fetchTopicMetadata( { topics: [ topic ] } );

            console.log( `[KAFKA] scalated [${topics[ 0 ].partitions?.length }] partitions to topic [${topic}]` );

        } else console.log( `[KAFKA] topic [${topic}] has already [${partitions}] partitions` );


    } catch( error ){

        throw new Error( `managePartitions -> ${error?.message}` );

    } finally{

        await admin?.disconnect();

    }

}

module.exports = {
    manageKafka
};