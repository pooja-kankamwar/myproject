const BaseModel = require("./baseModel");
const utils = require('../config/utils');
const { convertArrayToString } = require('../utils');
const moment = require('moment');

const {constants: {DATABASE: {RESEARCH_RESPONSE_DB}}} = require('../constants')


/**
 * Class representing a message model.
 * @class
 */
class EdroDashboardModel extends BaseModel {
    /**
     * Constructor.
     *
     * @param  {Object}  opts
     */
    constructor( opts ) {
        super( opts );
        this._hasTimestamps = false;
        this.clientId = opts.clientId;
    }

    async getActivityResponseData(params) {

        const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_RESPONSE_DB);

        const bindingParams = []
        if (params.participantsIds) {
            params.participantsIds = convertArrayToString(params.participantsIds);
        }
        if (params.siteIds) {
            params.siteIds = convertArrayToString(params.siteIds);
        }
        let limitParams=''
        if(params.offset !=null && params.limit !=null){
            limitParams= `LIMIT ${params.offset},${params.limit}`
        }
        let orderByParams=''
        if (params.sortValue && params.sortOrder) {
            orderByParams = `ORDER BY ${params.sortValue} ${params.sortOrder}`
        }

      let sqlQuery = `SELECT 
                        ar.participant_id, 
                        ar.response_data, 
                        ar.end_time, 
                        ar.start_time, 
                        CASE WHEN pt.user_defined_participant_id is not null THEN pt.user_defined_participant_id else pt.id end AS participantId 
                        from 
                        research.participant pt 
                        JOIN research_response.activity_response ar ON ar.participant_id = pt.id 
                        WHERE 
                        ar.study_id = '${params.studyId}'
                        ${params.siteIds ? `AND ar.site_id IN (${params.siteIds}) ` : '' } 
                        AND ar.activity_id = '${params.activityId}' 
                        ${params.participantsIds ? `AND pt.id IN (${params.participantsIds}) ` : '' } 
                        ${params.fromDate ? `and end_time BETWEEN '${params.fromDate}' AND '${params.toDate}'` : '' }
                        ${orderByParams} 
                        ${limitParams}`

        
        try {
            console.log(`getRecentData query SQL ${JSON.stringify(params)} \n${sqlQuery}`);
            const [data] = await dbConnectionPool.query(sqlQuery, bindingParams)
            dbConnectionPool.end();
            return data;
        } catch (error) {
            dbConnectionPool.end();
            console.log('Error in function getRecentData:', error);
            throw error;
        }
    }
    
    async getExpectedData(params) {
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId,RESEARCH_RESPONSE_DB);
      try {
        if (params.participantsIds) {
            params.participantsIds = convertArrayToString(params.participantsIds);
           }
        if (params.siteIds) {
           params.siteIds = convertArrayToString(params.siteIds);
          }
        let querySql = `SELECT
                        pt.start_date_utc,
                        pt.study_id as study_id,
                            CASE
                                WHEN pt.user_defined_participant_id is not null
                                    THEN pt.user_defined_participant_id
                                else pt.id
                                end   AS participantId,
                                pt.id as participant_id,
                            CASE
                                WHEN exists(
                                        SELECT (md.study_end_date)
                                        FROM research.study_meta_data md
                                        WHERE md.id = pt.study_id
                                        ${(params.fromDate && params.toDate) ? `and  DATE_ADD(md.study_end_date, INTERVAL 1 DAY) between '${params.fromDate} 00:00:00' AND '${params.toDate} 23:59:59'` : 'and  DATE_ADD(md.study_end_date, INTERVAL 1 DAY)<=now()'}
                                    ) THEN 'false'
                                WHEN pt.status in ('WITHDRAWN', 'DISQUALIFIED', 'DISQUALIFY','WITHDRAWSTUDY', 'DISCONTINUED') THEN 'false'
                                WHEN pt.status in ('ACTIVE') THEN 'true' END AS is_participant_active,
                            (
                                SELECT Max(ar1.end_time)
                                FROM research_response.activity_response ar1
                                WHERE ar1.participant_id = pt.id
                                    ${(params.toDate) ? `and (case when ar1.end_time <= date_add('${params.toDate}',INTERVAL 1 DAY) then ar1.end_time else null end)` : ''}
                                    and ar1.activity_id = '${params.activityId}'
                            ) as most_recent_data,
                            CASE WHEN pt.status in ( 'WITHDRAWN', 'DISQUALIFIED', 'DISQUALIFY','WITHDRAWSTUDY', 'DISCONTINUED')
                                    THEN
                                    (
                                        SELECT DATE_ADD(
                                                        pt1.start_date_utc, INTERVAL pts1.end_day DAY
                                                    ) as discontinued_date
                                        FROM research.participant pt1
                                                    JOIN research_response.participant_task_schedule pts1 ON pts1.participant_id = pt1.id
                                        WHERE pts1.participant_id = pts.participant_id
                                            AND pts1.task_id = pts.task_id
                                            ${(params.fromDate && params.toDate) ? `AND DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day DAY) between '${params.fromDate} 00:00:00' AND '${params.toDate} 23:59:59'` : 'AND DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day DAY)<=now()'}
                                            AND DATE_ADD(
                                                        pt1.start_date_utc, INTERVAL pts1.end_day DAY
                                                    ) <= (
                                                    select date(max(modified_time)) as discontinued_date
                                                    from research.participant_status_history psh
                                                    where new_status in ('DISQUALIFIED', 'WITHDRAWSTUDY') and psh.participant_id = pts.participant_id
                                                )
                                            AND NOT EXISTS(
                                                SELECT ar2.task_instance_id
                                                FROM research_response.activity_response ar2
                                                WHERE ar2.participant_id = pts.participant_id
                                                    AND ar2.task_instance_id = pts1.task_instance_id
                                            )
                                        ORDER BY discontinued_date desc limit 1
                                    )
                                ELSE
                                    (
                                        SELECT DATE_ADD(
                                                        pt1.start_date_utc, INTERVAL pts1.end_day DAY
                                                    ) as missed_date
                                        FROM research.participant pt1
                                                    JOIN research_response.participant_task_schedule pts1 ON pts1.participant_id = pt1.id
                                        WHERE pts1.participant_id = pts.participant_id
                                            AND pts1.task_id = pts.task_id
                                            ${(params.fromDate && params.toDate) ? `AND DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day DAY) between '${params.fromDate} 00:00:00' AND '${params.toDate} 23:59:59'` : 'AND DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day DAY) <=now()'}
                                            AND NOT EXISTS(
                                                SELECT ar2.task_instance_id
                                                FROM research_response.activity_response ar2
                                                WHERE ar2.participant_id = pts.participant_id
                                                    AND ar2.task_instance_id = pts1.task_instance_id
                                            )
                                        ORDER BY missed_date desc
                                        limit 1
                                    )
                                END as expected_missing_date,
                            (
                                SELECT count(pt1.id)
                                FROM research.participant pt1
                                        JOIN research_response.participant_task_schedule pts1 ON pts1.participant_id = pt1.id
                                WHERE pts1.participant_id = pts.participant_id
                                    AND pts1.task_id = pts.task_id
                                    AND IF(
                                            pt.status in ('WITHDRAWN', 'DISQUALIFIED', 'DISQUALIFY', 'WITHDRAWSTUDY', 'DISCONTINUED'),
                                            (DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day DAY) <= (
                                                select date(max(modified_time)) as discontinued_date
                                                from research.participant_status_history psh
                                                where new_status in ('DISQUALIFIED', 'WITHDRAWSTUDY')
                                                    and psh.participant_id = pts.participant_id
                                            )${(params.fromDate && params.toDate) ? `AND DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day  DAY) between '${params.fromDate} 00:00:00' AND '${params.toDate} 23:59:59'),` : 'AND DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day  DAY) <=now()),'}
                                            ${(params.fromDate && params.toDate) ? `DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day  DAY) between '${params.fromDate} 00:00:00' AND '${params.toDate} 23:59:59')` : 'DATE_ADD(pt1.start_date_utc, INTERVAL pts1.end_day  DAY)<=now())'}
                                    AND NOT EXISTS(
                                        SELECT ar2.task_instance_id
                                        FROM research_response.activity_response ar2
                                        WHERE ar2.participant_id = pts.participant_id
                                            AND ar2.task_instance_id = pts1.task_instance_id
                                    )
                            )  days_missing
                    FROM research.participant pt
                    Right JOIN research_response.participant_task_schedule pts ON pts.participant_id = pt.id
                    WHERE pt.study_id = '${params.studyId}'
                        AND pts.task_id = '${params.activityId}'
                        ${params.siteIds ? `AND pt.site_id IN (${params.siteIds}) ` : ''}
                        ${params.participantsIds ? `AND pts.participant_id IN (${params.participantsIds}) ` : ''}              
                        AND pt.start_date_utc <= ${(params.toDate) ? `Date('${params.toDate}')` : 'Date(now())'}
                        AND pt.status in ('ACTIVE', 'WITHDRAWN', 'DISQUALIFIED','DISQUALIFY', 'WITHDRAWSTUDY', 'DISCONTINUED')

                        ${(params.fromDate) ? `AND NOT EXISTS
                        ( select modified_time as discontinued_date
                                                    from research.participant_status_history psh
                                                    where new_status in ('DISQUALIFIED', 'WITHDRAWSTUDY')
                                                    and modified_time< Date('${params.fromDate}')
                                                    and psh.participant_id = pt.id)` : ''}
                    GROUP BY pts.participant_id
                    ORDER BY expected_missing_date desc,most_recent_data desc, participantId asc `;
       utils.createLog('',`getExpectedData query SQL ${JSON.stringify(params)}`,`${querySql}`);
       const [data] = await dbConnectionPool.query(querySql);
       dbConnectionPool.end();
       return data;
      } catch (error) {
        dbConnectionPool.end();
        utils.createLog('', `Error in function getExpectedData`, error);
        throw error;
      }
    }

   }

module.exports = EdroDashboardModel;
