const {Pg} = require('../packages');
const secretsManager = require('../config/secretsManager');
/**
 * define base model
 */
class RedshiftModel {

    /**
    * Define base model constructor.
    */
    constructor() {
        this._hasTimestamps = false;
    }

    /**
     * Get the table used for this model.
     *
     * @returns {string} The database table used.
     */
    static get table() {
        return this._table;
    }

    /**
     * Set the table used for this model.
     *
     * @param {string} t The database table to be used.
     */
    static set table( t ) {
        this._table = t;
    }

    /**
     * Get the hasTimestamps used for this model.
     *
     * @returns {string} The hasTimestamps setting.
     */
    static get hasTimestamps() {
        return this._hasTimestamps;
    }

    /**
     * Set the hasTimestamps used for this model.
     *
     * @param {string} t The hasTimestamps setting.
     */
    static set hasTimestamps( t ) {
        this._hasTimestamps = t;
    }

    async _initRedshiftDbConnectionPool(clientId, dbName) {
        try {
            const connectionConfig = await secretsManager.getRedshiftConnectionConfig(clientId);
            const configArr = connectionConfig.url.split(':');
            const hostName = configArr[2].replace('//', '');
            const port = configArr[3].replace(/[^0-9]+/, '');
            const dataBaseName = configArr[3].replace(/[0-9]+\//, '');
            if (dbName) {
                dataBaseName = dbName;
            }

            const clientRedshift = new Pg.Pool({
                host: hostName,
                user: connectionConfig.username,
                password: connectionConfig.password,
                port: port,
                database: dataBaseName
            });
            return clientRedshift;
        } catch (error) {
            console.log('Error in redshift_initRedshiftDbConnectionPool:', error);
            throw error;
        }
    }
}
module.exports = RedshiftModel;
