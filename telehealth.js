const BaseModel = require("./baseModel");
const utils = require('../config/utils');
const {
  constants: {
    DATABASE: { RESEARCH_RESPONSE_DB },
  },
} = require("../constants");
const { convertArrayToString } = require("../utils");

/**
 * Class representing a message model.
 * @class
 */
class TelehealthModel extends BaseModel {
  /**
   * Constructor.
   *
   * @param  {Object}  opts
   */
  constructor(opts) {
    super(opts);
    this._hasTimestamps = false;
    this.clientId = opts.clientId;
  }

  async getTelehealthVisitsDuration(params) {
    const dbConnection = await this._initDbConnectionPool(
      this.clientId,
      RESEARCH_RESPONSE_DB
    );
    try {
      if (params.participantsIds) {
        params.participantsIds = convertArrayToString(params.participantsIds);
      }
      let query = `select TS.study_id,
      TS.site_id,
        (  select ST.name from research.site ST
                left join research.study_site p on p.site_id = ST.id
                where p.site_id= TS.site_id
        ) as site_name,
        (select c.country_name from   research.participant p
          left join research.country c on p.country_id = c.country_id
          left join research.site_country sc on sc.country_id = p.country_id
          where TS.study_id = p.study_id
          and sc.site_id =TS.site_id
          limit 1
          ) as country_name,
            TS.participant_id,
            EXTRACT(YEAR FROM TS.start_time)                  as Year,
            EXTRACT(MONTH FROM TS.start_time)                 as Month,
            TS.start_time,
            TS.end_time,
            TS.initiated_by                                   as Generic_Role,
            ROUND(TIME_TO_SEC(TIMEDIFF(TS.end_time, TS.start_time))/60)      as call_duration,
            CAST(avg(ROUND(TIME_TO_SEC(TIMEDIFF(TS.end_time, TS.start_time))/60)) AS DECIMAL(11,2)) as Monthly_Average,
            GREATEST(MIN(ROUND(TIME_TO_SEC(TIMEDIFF(TS.end_time, TS.start_time))/60)),0) as Monthly_Min,
            GREATEST(MAX(ROUND(TIME_TO_SEC(TIMEDIFF(TS.end_time, TS.start_time))/60)),0) as Monthly_Max,
            sum(ROUND(TIME_TO_SEC(TIMEDIFF(TS.end_time, TS.start_time)) /60))  as Monthly_total,
            count(*) as Monthly_count
            from research_response.telehealth_session TS
        Where ROUND(TIME_TO_SEC(TIMEDIFF(TS.end_time, TS.start_time))/60) is not null
        and session_status = 'complete'
      and TS.study_id= '${params.studyId}'
      and site_id  <> ''
      ${params.participantsIds ? `and TS.participant_id IN (${params.participantsIds}) ` : ''}
      ${params.siteId ? `and TS.site_id =  '${params.siteId}'` : ''}
      ${(params.fromDate && params.toDate) ? `and date(start_time) between '${params.fromDate}' and '${params.toDate}' `  : ''}  
      group by TS.site_id,Year, Month
      order by Year, Month;`;
      utils.createLog('', `getTelehealthVisitsDuration querySql`, query);
      const [data] = await dbConnection.query(query);
      utils.createLog('', `getTelehealthVisitsDuration Data`, data);
      dbConnection.end();
      return data;
    } catch (error) {
      dbConnection.end();
      utils.createLog('', `Error in function getTelehealthVisitsDuration:`, error);
      throw error;
    }
  }

  async getCancelledTelehealthVisits(params) {
    const dbConnection = await this._initDbConnectionPool(
      this.clientId,
      RESEARCH_RESPONSE_DB
    );
    try {
      if (params.participantsIds) {
        params.participantsIds = convertArrayToString(params.participantsIds);
      }

      let querySql = `
        SELECT 
        PPA.study_id, 
        PPA.site_id, 
        PPA.participant_id, 
        PPA.start_time, 
        PPA.end_time, 
        (
          select 
            ST1.name 
          from 
            research.site ST1 
            left join research.study_site p on p.site_id = ST1.id 
          where 
            p.site_id = PPA.site_id
        ) as siteName, 
        (
          select 
            c.country_name 
          from 
            research.participant p 
            left join research.country c on p.country_id = c.country_id 
            left join research.site_country sc on sc.country_id = p.country_id 
          where 
            PPA.study_id = p.study_id 
            and sc.site_id = PPA.site_id 
          limit 
            1
        ) as country_name, 
        CASE When RT.cancelled_by in (
          'Pi', 'SI', 'SC', 'subpi', 'Interviewer', 
          'CallCenter', 'StudyCoordinator'
        ) 
        OR PASH.changed_by_role in (
          'Pi', 'SI', 'SC', 'subpi', 'Interviewer', 
          'CallCenter', 'StudyCoordinator'
        ) Then 'Site_Team' When RT.cancelled_by in ('HH', 'HomeHealth') 
        OR PASH.changed_by_role in ('HH', 'HomeHealth') Then 'HomeHealth' When RT.cancelled_by = 'Participant' 
        OR PASH.changed_by_role = 'Participant' Then 'Participant' END as categories, 
        CASE When RT.cancelled_by is not null Then RT.cancelled_by When PASH.changed_by_role is not null Then PASH.changed_by_role END as cancelled_by, 
        COUNT(PPA.id) canceled, 
        (
          SELECT 
            count(pa.id) 
          FROM 
            research_response.pi_participant_appointment pa 
          where 
            pa.study_id = PPA.study_id 
            AND pa.site_id = PPA.site_id 
            and pa.visit_type in ('Telehealth', 'Phonecall') 
            and pa.status not in ('NotStarted', 'Reschedule') 
     
            ${params.participantsIds ? `and pa.participant_id IN (${params.participantsIds}) ` : ''}
            ${(params.fromDate && params.toDate) ? `and date(pa.start_time) between '${params.fromDate}' and '${params.toDate}' `  : ''}  
       
            ) AS total_visit_count 
      FROM 
        research_response.pi_participant_appointment PPA 
        Left JOIN research_response.telehealth_session RT ON RT.appointment_id = PPA.id 
        ${(params.fromDate && params.toDate) ? `and date(RT.end_time) between '${params.fromDate}' and '${params.toDate}' `  : ''}  
        Left JOIN research_response.participant_appointment_status_history PASH ON PASH.appointment_id = PPA.id 
        ${(params.fromDate && params.toDate) ? `and date(PASH.changed_on) between '${params.fromDate}' and '${params.toDate}' `  : ''}  
        and PASH.new_status IN ('SiteCancelled', 'Cancelled') 
      where 
        PPA.study_id = '${params.studyId}' 
        and PPA.site_id <> ''
        and PPA.status IN ('SiteCancelled', 'Cancelled')
        ${params.participantsIds ? `and PPA.participant_id IN (${params.participantsIds}) ` : ''}
        ${params.siteId ? `and PPA.site_id = '${params.siteId}' ` : ''}
        ${(params.fromDate && params.toDate) ? `and date(PPA.start_time) between '${params.fromDate}' and '${params.toDate}' `  : ''}  
        AND NOT (
          NOT RT.cancelled_by <> '' 
          AND NOT PASH.changed_by_role <> ''
        ) 
        group by 
        PPA.site_id, 
        categories;
      `;
      utils.createLog('', `getCancelledTelehealthVisits querySql`, querySql);
      const [data] = await dbConnection.query(querySql);
      utils.createLog('', `getCancelledTelehealthVisits Data`, data);
      dbConnection.end();
      return data;
    } catch (error) {
      dbConnection.end();
      utils.createLog('', `Error in function getCancelledTelehealthVisits`, error);
      throw error;
    }
  }
}

module.exports = TelehealthModel;
