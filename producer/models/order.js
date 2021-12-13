const bd = require( "../config/database/postgres" )

const getOrder = async () => {

    try {

        const { rows = [] } = await bd.query( `
            SELECT
                *
            FROM
                "order"
        ` );

        return rows;

    } catch ( error ) {

        throw new Error( `getOrder -> ${error?.message}` );

    }

}

module.exports = {
    getOrder
}