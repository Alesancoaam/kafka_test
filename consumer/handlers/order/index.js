const sleep = ( ms ) => new Promise( ( resolve ) => setTimeout( resolve, ms ) );

const orderHandler = async( { topic, partition, message } ) =>{

    try{
        
        const orderMessage = JSON.parse( message.value.toString() );

        console.log( orderMessage );

        return true;

    } catch( error ){

        throw new Error( `orderHandler -> ${error?.message}` );

    }

}

module.exports = {
    orderHandler
}