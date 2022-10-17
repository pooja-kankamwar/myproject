const BaseModel = require("./baseModel");
const RedshiftModel = require("./redshiftModel");
const {constants: {DATABASE: {RESEARCH_DB}}} = require('../constants')
const {convertArrayToString} = require('../utils')


/**
 * Class representing a message model.
 * @class
 */
class EngagementModel extends BaseModel {
    /**
     * Constructor.
     *
     * @param  {Object}  opts
     */
    constructor( opts ) {
        super( opts );
        this.table = "enrollment";
        this._hasTimestamps = false;
        this.clientId = opts.clientId;
    }

    async getDeviceUsage(params){
        const redshiftModel = new RedshiftModel();
        const redshiftConnectionPool = await redshiftModel._initRedshiftDbConnectionPool(this.clientId);
        try {
            let participantsIds = null;
            if (params.participantsIds) {
                participantsIds = convertArrayToString(params.participantsIds);
            }

            // Redshift Query
            let redshiftQuery = `SELECT max(device_os)        as device_os,
            MAX(study_id)         as study_id,
            MAX(participant_id)   as participant_id,
            avg(session_duration) as session_duration
            FROM (SELECT max(lower(device_platform_name))          as device_os,
                        MAX(a_study_id)                           as study_id,
                        MAX(a_participant_id)                     as participant_id,
                        max(session_id)                           as session_id,
                        DATEDIFF(second,
                                MIN(event_timestamp),
                                MAX(CASE
                                        WHEN event_type = '_session.stop' THEN session_stop_timestamp
                                        ELSE event_timestamp END)) as session_duration
                FROM awsma.event
                WHERE session_id != '00000000-00000000'
                    and a_participant_id IS NOT NULL
                    and a_study_id = '${params.studyId}'
                    and device_platform_name is not null
                ${params.participantsIds ? ` and a_participant_id IN (${participantsIds}) ` :''}
                ${params.siteId ? ` and a_site_id = '${params.siteId}' ` :''}
                ${params.fromDate ? ` and DATE(event_timestamp) between '${params.fromDate}' and '${params.toDate}' ` :''}
                GROUP BY a_participant_id, session_id) a
            GROUP BY participant_id;`;
            console.log(`getDeviceUsage Redshift Query: ${redshiftQuery}`);
            console.time(`getDeviceUsage Redshift SQL ${JSON.stringify(params)} Ends in:`)
            const cursor = await redshiftConnectionPool.query(redshiftQuery);
            console.timeEnd(`getDeviceUsage Redshift SQL ${JSON.stringify(params)} Ends in:`)
            const data = cursor.rows;
            redshiftConnectionPool.end();
            console.log(`Data Redshift Query: ${JSON.stringify(data)}`);
            
            return data;
        } catch (error) {
            redshiftConnectionPool.end();
            console.log('Error in function getPlatformInsight-DeviceUsage:', error);
            throw error;
        }
    }

    async getPlatformInsight(params){
        const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_DB);
        try {
            let participantsIds = null;
            if (params.participantsIds) {
                participantsIds = convertArrayToString(params.participantsIds);
            }

            const bindingParams = [params.studyId];
            let querySql = `
            SELECT
            PSC.study_id,
            PSC.participant_id, 
            PSC.country_id,
            PSC.country_name as countryName,
            PSC.site_id,
            PSC.site_name as siteName,
            IF(P.status = 'ACTIVEWMODIFICATION', 'Active With Modification', P.status) as status
            FROM research_analytics.participant_site_country PSC
            LEFT JOIN research.participant P ON PSC.participant_id = P.id
            WHERE PSC.study_id = ?
            and P.status IN ('ACTIVE', 'ACTIVEWMODIFICATION', 'INVITED', 'REGISTERED', 'VERIFIED')
            ${params.participantsIds ? `and PSC.participant_id IN (${participantsIds}) ` : ''} 
            ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
            GROUP BY participant_id, country_id, site_id, status
            `;
            console.time(`getPlatformInsight mySQL SQL ${JSON.stringify(params)} Ends in:`)
            const [data] = await dbConnectionPool.query(querySql, bindingParams)
            console.time(`getPlatformInsight mySQL SQL ${JSON.stringify(params)} Ends in:`)
            console.log(`getPlatformInsight mySQL Query: ${querySql}`);
            dbConnectionPool.end();
            console.log(`Data mySQL Query: ${JSON.stringify(data)}`);
            return data;
        } catch (error) {
            dbConnectionPool.end();
            console.log('Error in function getPlatformInsight:', error);
            throw error;
        }
    }
    
}

module.exports = EngagementModel;
