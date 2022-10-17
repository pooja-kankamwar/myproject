const {mySql} = require('../packages');
const secretsManager = require('../config/secretsManager');
/**
 * define base model
 */
class BaseModel {

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

    setBindingParams(bindingParams, param) {
      bindingParams.push(param);
      return '?'
    }

    async _initDbConnectionPool(clientId, dbName) {
        try {
            const connectionConfig = await secretsManager.getMysqlConnectionConfig(clientId);
            let dataBaseName = connectionConfig.dbname;
            if (dbName) {
                dataBaseName = dbName;
            }

            const connection = await mySql.createConnection({
                host: connectionConfig.host,
                user: connectionConfig.username,
                password: connectionConfig.password,
                database: dataBaseName
            });
            return connection;
        } catch (error) {
            console.log('Error in baseModel_initDbConnectionPool:', error);
            throw error;
        }
    }

    async _initDbPPMDConnectionPool(clientId, dbName) {
      try {
        const connectionConfig = await secretsManager.getMysqlConnectionConfig(clientId, null, true);
        let dataBaseName = connectionConfig.dbname;
        if (dbName) {
            dataBaseName = dbName;
        }

        const connection = await mySql.createConnection({
            host: connectionConfig.host,
            user: connectionConfig.username,
            password: connectionConfig.password,
            database: dataBaseName
        });
        return connection;
      } catch (error) {
          console.log('Error in baseModel_initDbPPMDConnectionPool:', error);
          throw error;
      }
    }
}
module.exports = BaseModel;
