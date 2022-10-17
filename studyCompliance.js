const BaseModel = require("./baseModel");
const {constants: {DATABASE: {RESEARCH_ANALYTICS_DB}}} = require('../constants')
const {convertArrayToString} = require('../utils')
const utils = require('../config/utils');

/**
 * Class representing a message model.
 * @class
 */
class StudyComplianceModel extends BaseModel {
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

    async getCompliance (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        for (let index = 0; index < 7; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select study_id, country_id, country_name as countryName, site_id, site_name as siteName,
            compliance_date as date, year, month,
            GROUP_CONCAT(case when percent_compliance between 0 and 0.99 then activityName end SEPARATOR '||') as "compliance-0",
            GROUP_CONCAT(case when percent_compliance between 1 and 9.99 then activityName end SEPARATOR '||') as "compliance-1-10",
            GROUP_CONCAT(case when percent_compliance = 10.00 then activityName end SEPARATOR '||') as "compliance-10",
            GROUP_CONCAT(case when percent_compliance between 10.01 and 19.99 then activityName end SEPARATOR '||') as "compliance-10-20",
            GROUP_CONCAT(case when percent_compliance = 20.00 then activityName end SEPARATOR '||') as "compliance-20",
            GROUP_CONCAT(case when percent_compliance between 20.01 and 29.99 then activityName end SEPARATOR '||') as "compliance-20-30",
            GROUP_CONCAT(case when percent_compliance = 30.00 then activityName end SEPARATOR '||') as "compliance-30",
            GROUP_CONCAT(case when percent_compliance between 30.01 and 39.99 then activityName end SEPARATOR '||') as "compliance-30-40",
            GROUP_CONCAT(case when percent_compliance = 40.00 then activityName end SEPARATOR '||') as "compliance-40",
            GROUP_CONCAT(case when percent_compliance between 40.01 and 49.99 then activityName end SEPARATOR '||') as "compliance-40-50",
            GROUP_CONCAT(case when percent_compliance = 50.00 then activityName end SEPARATOR '||') as "compliance-50",
            GROUP_CONCAT(case when percent_compliance between 50.01 and 59.99 then activityName end SEPARATOR '||') as "compliance-50-60",
            GROUP_CONCAT(case when percent_compliance = 60.00 then activityName end SEPARATOR '||') as "compliance-60",
            GROUP_CONCAT(case when percent_compliance between 60.01 and 69.99 then activityName end SEPARATOR '||') as "compliance-60-70",
            GROUP_CONCAT(case when percent_compliance = 70.00 then activityName end SEPARATOR '||') as "compliance-70",
            GROUP_CONCAT(case when percent_compliance between 70.01 and 79.99 then activityName end SEPARATOR '||') as "compliance-70-80",
            GROUP_CONCAT(case when percent_compliance = 80.00 then activityName end SEPARATOR '||') as "compliance-80",
            GROUP_CONCAT(case when percent_compliance between 80.01 and 89.99 then activityName end SEPARATOR '||') as "compliance-80-90",
            GROUP_CONCAT(case when percent_compliance = 90.00 then activityName end SEPARATOR '||') as "compliance-90",
            GROUP_CONCAT(case when percent_compliance between 90.01 and 99.99 then activityName end SEPARATOR '||') as "compliance-90-100",
            GROUP_CONCAT(case when percent_compliance = 100.00 then activityName end SEPARATOR '||') as "compliance-100"
            FROM (
            select
                  study_id, concat(activity_name, '**', count(participant_id), '::', 
                  round(avg(percent_compliance), 0), '++', compliance_date) as activityName, 
                  country_id, country_name, site_id, site_name,
                  compliance_date, avg(percent_compliance) as percent_compliance,
                  EXTRACT(YEAR from compliance_date) as year,
                  EXTRACT(MONTH from compliance_date) as month
            FROM (
            select
            study_id,
            participant_id,
            country_id,
            country_name,
            site_id,
            site_name, 
            compliance_date,
            due_date,
            activity_name,
            COUNT(case when status = 'Complete' AND compliance_date < due_date then 1 end)/COUNT(case when status = 'Complete' then 1 end)*100 as percent_compliance
            from
              (select
              P.participant_id,
              P.site_name,
              P.site_id,
              P.study_name,
              P.study_id,
              P.country_id,
              P.country_name,
              P.task_title as activity_name,
              coalesce(DATE(P.completion_date),
                      DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as compliance_date,
              DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY) as due_date, 
              P.enrollment_date,
              CASE 
              WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
              WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
              WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
              WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
              WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
              WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
              WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
              WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
              WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
              END AS status
                from
                  (select
                  PSC.participant_id,
                  PSC.participant_start_date,
                  PSC.site_name,
                  PSC.site_id,
                  PSC.study_name,
                  PSC.study_id,
                  PSC.country_id,
                  PSC.country_name,
                  PSC.enrollment_date,
                  PTS.task_title,
                  pts.created_date,
                  pts.start_day,
                  pts.end_day,
                  pts.schedule_day,
                  case 
                  when pts.task_type in ('survey', 'epro') then srt.id
                  when pts.task_type = 'telehealth' then ppa.id
                  when pts.task_type = 'activity' then ar.id
                  end as ar_id,
                  case 
                  when pts.task_type in ('survey', 'epro') then 'survey'
                  when pts.task_type = 'telehealth' then 'visits'
                  when pts.task_type = 'activity' then 'activity'
                  end as response_type,
                  case 
                  when pts.task_type = 'telehealth' then ppa.status
                  else ''
                  end as visit_status,
                  case 
                  when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
                  when pts.task_type = 'telehealth' then ppa.end_time
                  when pts.task_type = 'activity' then ar.end_time
                  end as completion_date
                  from research_analytics.participant_site_country PSC
                    inner join research_response.participant_task_schedule PTS ON PSC.participant_id = PTS.participant_id
                          and PSC.study_id = PTS.study_id
                    LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                          and pts.task_instance_id = ar.task_instance_id
                          and pts.study_version_id = ar.study_version_id
                    LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                          and pts.task_instance_id = srt.task_instance_id
                          and pts.study_version_id = srt.study_version_id
                    LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                          and pts.task_instance_id = ppa.task_instanceuuid
                          and pts.study_version_id = ppa.study_version_id
                          and (ppa.visit_id IS NOT NULL)
                          where PSC.study_id = ? AND PTS.study_id = ?
                              ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
                              ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''}
                  ) P
                WHERE coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
                ${params.fromDate ? `and coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) between '${params.fromDate}' and '${params.toDate}' ` : ''} 
                ${params.fromDate ? `and P.enrollment_date BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
              ) T1
            WHERE status = 'Complete'
            GROUP BY participant_id, country_id, site_id, activity_name, compliance_date
            ) c
            GROUP BY activity_name, country_id, site_id, year, month
            ) A1
            GROUP BY country_id, site_id, year, month
            ORDER BY year, month asc
        `;
        console.log(`getCompliance query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getCompliance query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getCompliance query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length for getCompliance is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getCompliance]', er)
        throw er
      }
    }

    async getComplianceByParticipant (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        for (let index = 0; index < 11; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select study_id, country_id, country_name as countryName, site_id, site_name as siteName,
          compliance_date as date,  year,  month,
          COUNT(case when percent_compliance between 0 and 0.99 then 1 end) as "compliance-0",
          COUNT(case when percent_compliance between 1 and 9.99 then 1 end) as "compliance-1-10",
          COUNT(case when percent_compliance = 10.00 then 1 end) as "compliance-10",
          COUNT(case when percent_compliance between 10.01 and 19.99 then 1 end) as "compliance-10-20",
          COUNT(case when percent_compliance = 20.00 then 1 end) as "compliance-20",
          COUNT(case when percent_compliance between 20.01 and 29.99 then 1 end) as "compliance-20-30",
          COUNT(case when percent_compliance = 30.00 then 1 end) as "compliance-30",
          COUNT(case when percent_compliance between 30.01 and 39.99 then 1 end) as "compliance-30-40",
          COUNT(case when percent_compliance = 40.00 then 1 end) as "compliance-40",
          COUNT(case when percent_compliance between 40.01 and 49.99 then 1 end) as "compliance-40-50",
          COUNT(case when percent_compliance = 50.00 then 1 end) as "compliance-50",
          COUNT(case when percent_compliance between 50.01 and 59.99 then 1 end) as "compliance-50-60",
          COUNT(case when percent_compliance = 60.00 then 1 end) as "compliance-60",
          COUNT(case when percent_compliance between 60.01 and 69.99 then 1 end) as "compliance-60-70",
          COUNT(case when percent_compliance = 70.00 then 1 end) as "compliance-70",
          COUNT(case when percent_compliance between 70.01 and 79.99 then 1 end) as "compliance-70-80",
          COUNT(case when percent_compliance = 80.00 then 1 end) as "compliance-80",
          COUNT(case when percent_compliance between 80.01 and 89.99 then 1 end) as "compliance-80-90",
          COUNT(case when percent_compliance = 90.00 then 1 end) as "compliance-90",
          COUNT(case when percent_compliance between 90.01 and 99.99 then 1 end) as "compliance-90-100",
          COUNT(case when percent_compliance = 100.00 then 1 end) as "compliance-100" FROM (
            select
            study_id, participant_id, activity_name, country_id, country_name, site_id, site_name,
            EXTRACT(YEAR from compliance_date) as year,
            EXTRACT(MONTH from compliance_date) as month,
            compliance_date, 
            FLOOR(avg(percent_compliance)) as percent_compliance
        FROM (
        select
        study_id,
        participant_id,
        country_id,
        country_name,
        site_id,
        site_name, 
        compliance_date,
        due_date,
        activity_name,
        COUNT(case when status = 'Complete' AND compliance_date < due_date then 1 end)/COUNT(case when status = 'Complete' then 1 end)*100 as percent_compliance
        from
          (select
          P.participant_id,
          P.site_name,
          P.site_id,
          P.study_name,
          P.study_id,
          P.country_id,
          P.country_name,
          P.task_title as activity_name,
          coalesce(DATE(P.completion_date),
          DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as compliance_date,
          DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY) as due_date, 
          P.enrollment_date,
          CASE 
          WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
          WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
          WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
          WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
          WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
          WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
          WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
          WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          END AS status
            from
              (select
              PSC.participant_id,
              PSC.participant_start_date,
              PSC.site_name,
              PSC.site_id,
              PSC.study_name,
              PSC.study_id,
              PSC.country_id,
              PSC.country_name,
              PSC.enrollment_date,
              PTS.task_title,
              pts.created_date,
              pts.start_day,
              pts.end_day,
              pts.schedule_day,
              case 
              when pts.task_type in ('survey', 'epro') then srt.id
              when pts.task_type = 'telehealth' then ppa.id
              when pts.task_type = 'activity' then ar.id
              end as ar_id,
              case 
              when pts.task_type in ('survey', 'epro') then 'survey'
              when pts.task_type = 'telehealth' then 'visits'
              when pts.task_type = 'activity' then 'activity'
              end as response_type,
              case 
              when pts.task_type = 'telehealth' then ppa.status
              else ''
              end as visit_status,
              case 
              when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
              when pts.task_type = 'telehealth' then ppa.end_time
              when pts.task_type = 'activity' then ar.end_time
              end as completion_date
                from research_analytics.participant_site_country PSC
                inner join research_response.participant_task_schedule PTS ON PTS.study_id = ? and PSC.participant_id = PTS.participant_id
                  and PSC.study_id = PTS.study_id
                LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                      and pts.task_instance_id = ar.task_instance_id
                      and pts.study_version_id = ar.study_version_id
                LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                      and pts.task_instance_id = srt.task_instance_id
                      and pts.study_version_id = srt.study_version_id
                LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                      and pts.task_instance_id = ppa.task_instanceuuid
                      and pts.study_version_id = ppa.study_version_id
                      and (ppa.visit_id IS NOT NULL)
                 where PSC.study_id = ?
                          ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
                          ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''}
              ) P
              WHERE coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
                ${params.fromDate ? `and coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) between '${params.fromDate}' and '${params.toDate}' ` : ''} 
                ${params.fromDate ? `and P.enrollment_date BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
          ) T1
        WHERE status = 'Complete' 
        GROUP BY participant_id, country_id, site_id, activity_name, compliance_date
        ) c
        GROUP BY participant_id, country_id, site_id, year, month
        ) A1
        GROUP BY country_id, site_id, year, month
        ORDER BY year, month asc
        `;
        console.log(`getComplianceByParticipant query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getComplianceByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        console.timeEnd(`getComplianceByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length of getComplianceByParticipant is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getComplianceByParticipant]', er)
        throw er
      }
    }

    async getComplianceDetails (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let siteIds = null;
        let participantsIds = null;
        for (let index = 0; index < 9; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.siteIds) {
          siteIds = convertArrayToString(params.siteIds);
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select
          activity_name as activityName, max(compliance_date) as complianceDate, ROUND(avg(percent_compliance)) as percentCompliance
          FROM 
          (
            select * from research_analytics.participant_compliance where study_id = ? and compliance_date != " "
            ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
            ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
            ${params.fromDate ? `and compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''} 
            UNION
            SELECT * FROM
                (
                  select
                    study_id,
                    participant_id,
                    country_id,
                    country_name,
                    site_id,
                    site_name,
                    compliance_date,
                    activity_name,
                    IFNULL(
                      COUNT(
                        case when status = 'Complete'
                        AND DATE(compliance_date) <= schedule_date then 1 end
                      )/ COUNT(
                        case when status = 'Complete' then 1 end
                      )* 100,
                      0
                    ) as percent_compliance
                  from
                    (
                      select
                        pts.participant_id,
                        PTS.study_id,
                        psc.country_id,
                        psc.country_name,
                        psc.site_id,
                        psc.site_name,
                        ar.end_time as compliance_date,
                        pts.end_day,
                        pts.start_day,
                        pts.schedule_day,
                        ar.id ar_id,
                        pts.task_title as activity_name,
                        DATE_ADD(
                          psc.participant_start_date, INTERVAL PTS.schedule_day DAY
                        ) as schedule_date,
                        'Complete' as status
                      FROM
                      research_response.activity_response ar 
                        left join research_response.participant_task_schedule pts on pts.participant_id = ar.participant_id
                        and pts.task_instance_id = ar.task_instance_id
                        and pts.study_version_id = ar.study_version_id
                        left join research_analytics.participant_site_country psc on psc.participant_id = pts.participant_id
                        and psc.study_id = pts.study_id
                      where
                        pts.task_type = 'activity'
                        and ar.study_id = ?
                        and psc.study_id = ?
                        ${params.siteId ? `and psc.site_id = '${params.siteId}' ` : ''}
                        ${params.participantsIds ? `and psc.participant_id in (${participantsIds}) ` : ''} 
                        ${params.fromDate ? `and ar.end_time between '${params.fromDate}' and '${params.toDate}' ` : ''} 
                    ) t1
                  group by
                    country_id,
                    site_id,
                    participant_id,
                    activity_name,
                    compliance_date
                  order by
                    compliance_date asc
                ) A
          ) T1
          group by activity_name
          order by percent_compliance desc
        `;
        console.log(`getComplianceDetails query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getComplianceDetails query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getComplianceDetails query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length of getComplianceDetails is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getComplianceDetails]', er)
        throw er
      }
    }

    async getComplianceDetailsByParticipant (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let siteIds = null;
        let participantsIds = null;
        for (let index = 0; index < 10; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.siteIds) {
          siteIds = convertArrayToString(params.siteIds);
        }
        let querySql = `
        select
          participant_id as participantId, max(compliance_date) as complianceDate, FLOOR(avg(percent_compliance)) as percentCompliance
            FROM (
            select
            study_id,
            participant_id,
            country_id,
            country_name,
            site_id,
            site_name, 
            compliance_date,
            due_date,
            activity_name,
            COUNT(case when status = 'Complete' AND compliance_date < due_date then 1 end)/COUNT(case when status = 'Complete' then 1 end)*100 as percent_compliance
            from
              (select
              P.participant_id,
              P.site_name,
              P.site_id,
              P.study_name,
              P.study_id,
              P.country_id,
              P.country_name,
              P.task_title as activity_name,
              coalesce(DATE(P.completion_date),
                      DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as compliance_date,
              DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY) as due_date, 
              P.enrollment_date,
              CASE 
              WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
              WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
              WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
              WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
              WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
              WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
              WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
              WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
              WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
              END AS status
                from
                  (select
                  PSC.participant_id,
                  PSC.participant_start_date,
                  PSC.site_name,
                  PSC.site_id,
                  PSC.study_name,
                  PSC.study_id,
                  PSC.country_id,
                  PSC.country_name,
                  PSC.enrollment_date,
                  PTS.task_title,
                  pts.created_date,
                  pts.start_day,
                  pts.end_day,
                  pts.schedule_day,
                  case 
                  when pts.task_type in ('survey', 'epro') then srt.id
                  when pts.task_type = 'telehealth' then ppa.id
                  when pts.task_type = 'activity' then ar.id
                  end as ar_id,
                  case 
                  when pts.task_type in ('survey', 'epro') then 'survey'
                  when pts.task_type = 'telehealth' then 'visits'
                  when pts.task_type = 'activity' then 'activity'
                  end as response_type,
                  case 
                  when pts.task_type = 'telehealth' then ppa.status
                  else ''
                  end as visit_status,
                  case 
                  when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
                  when pts.task_type = 'telehealth' then ppa.end_time
                  when pts.task_type = 'activity' then ar.end_time
                  end as completion_date
                    from (
                      select participant_id,
                      participant_start_date, site_name, site_id, study_name, study_id, country_id, country_name from research_analytics.participant_site_country
                      where study_id = ?
                      ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                    ) PSC
                    INNER JOIN research_response.participant_task_schedule PTS ON PTS.study_id = ? and PSC.participant_id = PTS.participant_id
                      and PSC.study_id = PTS.study_id
                    LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                          and pts.task_instance_id = ar.task_instance_id
                          and pts.study_version_id = ar.study_version_id
                    LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                          and pts.task_instance_id = srt.task_instance_id
                          and pts.study_version_id = srt.study_version_id
                    LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                          and pts.task_instance_id = ppa.task_instanceuuid
                          and pts.study_version_id = ppa.study_version_id
                          and (ppa.visit_id IS NOT NULL)
                      where PSC.study_id = ?
                          ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
                          ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''}
                  ) P
                  WHERE coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
                    ${params.fromDate ? `and coalesce(DATE(P.completion_date),
                    DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) between '${params.fromDate}' and '${params.toDate}' ` : ''} 
                    ${params.fromDate ? `and P.enrollment_date BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
              ) T1
            WHERE status = 'Complete'
            GROUP BY participant_id, country_id, site_id, activity_name, compliance_date
            ) c
            GROUP BY participant_id
        `;
        console.log(`getComplianceDetailsByParticipant query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getComplianceDetailsByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getComplianceDetailsByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length of getComplianceDetailsByParticipantis: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getComplianceDetailsByParticipant]', er)
        throw er
      }
    }

    async getComplianceRiskScore (params) { 
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = [params.studyId];
        bindingParams.push(params.studyId);
        bindingParams.push(params.studyId);
        let participantsIds = null;
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select study_id, country_id, site_id, country_name as countryName, site_name as siteName, activity_name as activityName,
        month(compliance_date) as month, year(compliance_date) as year,
        percent_compliance,(100 - percent_compliance) as nonCompliance
        from 
        (
          select * from research_analytics.participant_compliance where study_id = ? and compliance_date != " "
          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
          ${params.fromDate ? `and compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''} 
          UNION
          SELECT * FROM
              (
                select
                  study_id,
                  participant_id,
                  country_id,
                  country_name,
                  site_id,
                  site_name,
                  compliance_date,
                  activity_name,
                  IFNULL(
                    COUNT(
                      case when status = 'Complete'
                      AND DATE(compliance_date) <= schedule_date then 1 end
                    )/ COUNT(
                      case when status = 'Complete' then 1 end
                    )* 100,
                    0
                  ) as percent_compliance
                from
                  (
                    select
                      pts.participant_id,
                      PTS.study_id,
                      psc.country_id,
                      psc.country_name,
                      psc.site_id,
                      psc.site_name,
                      ar.end_time as compliance_date,
                      pts.end_day,
                      pts.start_day,
                      pts.schedule_day,
                      ar.id ar_id,
                      pts.task_title as activity_name,
                      DATE_ADD(
                        psc.participant_start_date, INTERVAL PTS.schedule_day DAY
                      ) as schedule_date,
                      'Complete' as status
                    FROM
                    research_response.activity_response ar 
                      left join research_response.participant_task_schedule pts on pts.participant_id = ar.participant_id
                      and pts.task_instance_id = ar.task_instance_id
                      and pts.study_version_id = ar.study_version_id
                      left join research_analytics.participant_site_country psc on psc.participant_id = pts.participant_id
                      and psc.study_id = pts.study_id
                    where
                      pts.task_type = 'activity'
                      and ar.study_id = ?
                      and psc.study_id = ?
                      ${params.siteId ? `and psc.site_id = '${params.siteId}' ` : ''}
                      ${params.participantsIds ? `and psc.participant_id in (${participantsIds}) ` : ''} 
                      ${params.fromDate ? `and compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''} 
                  ) t1
                group by
                  country_id,
                  site_id,
                  participant_id,
                  activity_name,
                  compliance_date
                order by
                  compliance_date asc
              ) A
        ) T1
        group by study_id, country_id, site_id, activity_name
        order by count(participant_id) desc
        limit 10`;
        console.log(`getComplianceRiskScore query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        console.log(`the length for getComplianceRiskScore is: ${JSON.stringify(data.length)}`);
        dbConnectionPool.end();
        return data;
      } catch (er) {
        dbConnectionPool.end();
        console.log('[Error in function getComplianceRiskScore]', er);
        throw er;
      }
    }

    async getGeodemographicCompliance (params) { 
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        let participantsIds = null;
        if (params.participantsIds) {
          if(params.participantsIds.length == 0) participantsIds = '""';
          else participantsIds = convertArrayToString(params.participantsIds);
        }       
        let querySql = `
          select 
          study_id, 
          siteName, 
          country_id, 
          countryName, 
          aa.site_id, 
          compliance_rate, 
          nParticipant, 
          IFNULL(s.city, '') as siteCity, 
          IFNULL(s.state, '') as siteState,
          total_participants, 
          IFNULL(round(((
                nParticipant / total_participants
              )* 100), 2) , 0) as participantPercentage 
        from 
          (
            select 
              study_id, 
              site_name as siteName, 
              country_id, 
              country_name as countryName, 
              d4.site_id, 
              round(
                avg(percent_compliance), 
                2
              ) as compliance_rate, 
              nParticipant, 
              (
                select 
                  count(
                    distinct(participant_id)
                  ) as nParticipant 
                from 
                  (
                    SELECT 
                      PSC.participant_id, 
                      site_id, 
                      active_date, 
                      date(discontinue_date) as discontinue_date 
                    FROM 
                      research_analytics.participant_site_country PSC 
                      left join (
                        select 
                          max(modified_time) as discontinue_date, 
                          participant_id 
                        from 
                          research.participant_status_history psh 
                        where 
                          new_status in('DISQUALIFIED', 'WITHDRAWSTUDY') 
                        group by 
                          participant_id
                      ) ACT on psc.participant_id = ACT.participant_id 
                    WHERE 
                      psc.study_id = '${params.studyId}' 
                      AND psc.active_date <> '' 
                      and date(psc.active_date) <= coalesce(${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` } , curdate()) 
                      and coalesce(${params.fromDate && params.toDate ? `'${params.fromDate}' ` : `NULL` },curdate()) <= coalesce(discontinue_date,curdate())
                      ) aa
              ) as total_participants 
            from 
              (
                select 
                  study_id, 
                  site_id, 
                  participant_id, 
                  country_id, 
                  country_name, 
                  site_name, 
                  compliance_date, 
                  due_date, 
                  IFNULL(COUNT(
                    case when status = 'Complete' 
                    AND compliance_date <= due_date
                    AND due_date < curdate() then 1 end
                  )/ (
                    COUNT(
                      case when status = 'Missed' then 1 end
                    )+ COUNT(
                      case when status = 'Complete' 
                      AND compliance_date <= due_date
                      AND due_date < curdate() then 1 end
                    )
                  ) * 100 , 0) as percent_compliance 
                from 
                  (
                    select 
                      P.participant_id, 
                      P.site_name, 
                      P.site_id, 
                      P.study_id, 
                      P.country_id, 
                      P.country_name, 
                      P.task_title as activity_name, 
                      coalesce(
                        DATE(P.completion_date), 
                        DATE_ADD(
                          P.participant_start_date, INTERVAL P.end_day - 1 DAY
                        )
                      ) as compliance_date, 
                      DATE_ADD(
                        P.participant_start_date, INTERVAL P.end_day - 1 DAY
                      ) as due_date, 
                      P.enrollment_date, 
                      P.participant_start_date, 
                      P.active_date, 
                      P.end_day, 
                      P.start_day, 
                      P.task_type, 
                      P.completion_date, 
                      CASE WHEN P.task_type = 'telehealth' 
                      AND P.ar_id IS NOT NULL 
                      AND P.visit_status != 'Reschedule' THEN 'Complete' WHEN P.task_type = 'telehealth' 
                      AND P.end_day <= (
                        TIMESTAMPDIFF(
                          day, 
                          P.participant_start_date, 
                          coalesce(
                            discontinue_date, 
                            curdate()
                          )
                        )
                      ) 
                      AND P.ar_id IS NULL THEN 'Missed' WHEN P.task_type != 'telehealth' 
                      AND P.ar_id IS NOT NULL THEN 'Complete' WHEN P.task_type != 'telehealth' 
                      AND P.end_day <= (
                        TIMESTAMPDIFF(
                          day, 
                          P.participant_start_date, 
                          coalesce(
                            discontinue_date, 
                            curdate()
                          )
                        )
                      ) 
                      AND P.ar_id IS NULL THEN 'Missed' END AS status 
                    from 
                      (
                        select 
                          PSC.participant_id, 
                          PSC.participant_start_date, 
                          PSC.site_name, 
                          PSC.site_id, 
                          PSC.study_name, 
                          PSC.study_id, 
                          PSC.country_id, 
                          PSC.country_name, 
                          PSC.enrollment_date, 
                          PTS.task_title, 
                          pts.start_day, 
                          pts.end_day, 
                          psc.active_date, 
                          PTS.task_type, 
                          ppa.status as visit_status, 
                          pts.task_instance_id, 
                          date(discontinue_date) as discontinue_date, 
                          case when pts.task_type in ('survey', 'epro') then srt.id when pts.task_type = 'telehealth' then ppa.id when pts.task_type = 'activity' then ar.id end as ar_id, 
                          case when pts.task_type in ('survey', 'epro') then srt.completion_time_utc when pts.task_type = 'telehealth' then ppa.end_time when pts.task_type = 'activity' then ar.end_time end as completion_date 
                        from 
                          research_analytics.participant_site_country PSC 
                          inner join research_response.participant_task_schedule PTS ON PSC.participant_id = PTS.participant_id 
                          and PSC.study_id = PTS.study_id 
                          left join (
                            select 
                              distinct(psh.participant_id), 
                              psh.modified_time as discontinue_date 
                            from 
                              research.participant_status_history psh 
                            where 
                              psh.new_status in('DISQUALIFIED', 'WITHDRAWSTUDY') 
                              and psh.study_id = '${params.studyId}' 
                          ) ACT on psc.participant_id = ACT.participant_id 
                          LEFT JOIN research_response.activity_response ar on pts.task_instance_id = ar.task_instance_id 
                          and pts.study_version_id = ar.study_version_id 
                          and pts.participant_id = ar.participant_id 
                          LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id 
                          and pts.task_instance_id = srt.task_instance_id 
                          and pts.study_version_id = srt.study_version_id 
                          LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id 
                          and pts.task_instance_id = ppa.task_instanceuuid 
                          and pts.study_version_id = ppa.study_version_id 
                          and (ppa.visit_id IS NOT NULL) 
                        where 
                          psc.study_id = '${params.studyId}' 
                          and pts.study_id = '${params.studyId}'  
                          and psc.active_date <> '' 
                          and pts.enabled = true 
                          ${params.participantsIds ? `and psc.participant_id IN (${participantsIds}) ` : ''}
                          ${params.siteId ? `and psc.site_id = '${params.siteId}' ` : ''}
                          ) P 
                    WHERE 
                      DATE_ADD(
                        P.participant_start_date, INTERVAL P.end_day - 1 DAY
                      ) <= coalesce(
                        DATE(discontinue_date), 
                        CURDATE()
                      ) 
                       ${(params.fromDate && params.toDate) ? `and DATE_ADD(P.participant_start_date, INTERVAL P.end_day-1 DAY) between '${params.fromDate}' and '${params.toDate}' `  : ''}
                      ) T1 
                group by 
                  participant_id, 
                  site_id
              ) d4 
              join (
                select 
                  count(
                    distinct(participant_id)
                  ) as nParticipant, 
                  site_id 
                from 
                  (
                    SELECT 
                      PSC.participant_id, 
                      site_id, 
                      active_date, 
                      date(discontinue_date) as discontinue_date 
                    FROM 
                      research_analytics.participant_site_country PSC 
                      left join (
                        select 
                          max(modified_time) as discontinue_date, 
                          participant_id 
                        from 
                          research.participant_status_history psh 
                        where 
                          new_status in('DISQUALIFIED', 'WITHDRAWSTUDY') 
                        group by 
                          participant_id
                      ) ACT on psc.participant_id = ACT.participant_id 
                    WHERE 
                      psc.study_id = '${params.studyId}'  
                      AND psc.active_date <> '' 
                      and date(psc.active_date) <= coalesce(${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` } , curdate()) 
                      and coalesce(${params.fromDate && params.toDate ? `'${params.fromDate}' ` : `NULL` },curdate()) <= coalesce(discontinue_date,curdate())
                      ) aa 
                group by 
                  site_id
              ) ACT on d4.site_id = ACT.site_id 
            group by 
              d4.site_id
          ) aa 
        left join research.site s ON s.id = aa.site_id         
        `;
        utils.createLog('', `getGeodemographicCompliance querySql`, querySql);
        const [data] = await dbConnectionPool.query(querySql);
        utils.createLog('', `getGeodemographicCompliance data`, data);
        return data;
      } catch (er) {
        utils.createLog('', `Error in function getGeodemographicCompliance`, er);
        throw er;
      } finally {
        utils.createLog('', `Connection closed in finally`);
        dbConnectionPool.end();
      }
    }

    async getComplianceChangeScore (params) { 
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        for (let index = 0; index < 2; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select study_id,
        participant_id, 
        country_id, 
        country_name as countryName, 
        site_id, 
        site_name as siteName, 
        activity_name as activityName,
        DATE_FORMAT(compliance_date,'%Y-%m') as date,
        month(compliance_date) as month,
        year(compliance_date) as year,
        percent_compliance,
        last_percent_compliance,
        case when last_percent_compliance = 0 and percent_compliance > 0 then 1
        when (percent_compliance - last_percent_compliance) / last_percent_compliance is null then 0
        else
        (percent_compliance - last_percent_compliance) / last_percent_compliance end as changePercentage
        from (  
        select T1.study_id,
        T1.participant_id, 
        T1.country_id, 
        T1.country_name, 
        T1.site_id, 
        T1.site_name, 
        T1.activity_name,
        T1.compliance_date,
        T1.percent_compliance,
        case when T2.percent_compliance is null then 0 else T2.percent_compliance end as last_percent_compliance
        from (
        select a.study_id,
        a.participant_id, 
        a.country_id, 
        a.country_name, 
        a.site_id, 
        a.site_name, 
        a.compliance_date, 
        a.activity_name, 
        a.percent_compliance,
        @num:=@num + 1 as rn
        FROM research_analytics.participant_compliance a, (select @num:=0) a1
        where a.study_id = ?
        ${params.activityName ? `and a.activity_name = '${params.activityName}' ` : ''}
        ${params.siteId ? `and a.site_id = '${params.siteId}' ` : ''}
        ${params.participantsIds ? `and a.participant_id IN (${participantsIds}) ` : ''} 
        ${params.fromDate ? `and a.compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''}
        ) T1  
        left join (
        select b.study_id,
        b.participant_id, 
        b.country_id, 
        b.country_name, 
        b.site_id, 
        b.site_name, 
        b.compliance_date, 
        b.activity_name, 
        b.percent_compliance,
        @num2:=@num2 + 1 as rn
        FROM research_analytics.participant_compliance b, (select @num2:=0) b1  
        where b.study_id = ?
        ${params.activityName ? `and b.activity_name = '${params.activityName}' ` : ''}
        ${params.siteId ? `and b.site_id = '${params.siteId}' ` : ''}
        ${params.participantsIds ? `and b.participant_id IN (${participantsIds}) ` : ''} 
        ${params.fromDate ? `and b.compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''}
        ) T2
        on T1.rn = T2.rn + 1
        ) T
        group by study_id, site_id, activity_name, month, year
        order by date asc
        `;
        console.log(`getComplianceChangeScore query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.log(`the length of getComplianceChangeScore is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getComplianceChangeScore]', er)
        throw er
      }
    }

    async getComplianceDeviation (params) { 
      const {studyId, siteId, activityName} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = [studyId]
        bindingParams.push(studyId)
        let participantsIds = null;
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select study_id,
        participant_id, 
        country_id, 
        country_name as countryName, 
        site_id, 
        site_name as siteName, 
        activity_name as activityName,
        DATE_FORMAT(compliance_date,'%Y-%m') as date,
        month(compliance_date) as month,
        year(compliance_date) as year,
        percent_compliance,
        stddev(percent_compliance) as standardDeviation
        from 
        (
          select * from research_analytics.participant_compliance where study_id = ? and compliance_date != " "
          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
          ${params.fromDate ? `and compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''} 
          UNION
          SELECT * FROM
              (
                select
                  study_id,
                  participant_id,
                  country_id,
                  country_name,
                  site_id,
                  site_name,
                  compliance_date,
                  activity_name,
                  IFNULL(
                    COUNT(
                      case when status = 'Complete'
                      AND DATE(compliance_date) <= schedule_date then 1 end
                    )/ COUNT(
                      case when status = 'Complete' then 1 end
                    )* 100,
                    0
                  ) as percent_compliance
                from
                  (
                    select
                      pts.participant_id,
                      PTS.study_id,
                      psc.country_id,
                      psc.country_name,
                      psc.site_id,
                      psc.site_name,
                      ar.end_time as compliance_date,
                      pts.end_day,
                      pts.start_day,
                      pts.schedule_day,
                      ar.id ar_id,
                      pts.task_title as activity_name,
                      DATE_ADD(
                        psc.participant_start_date, INTERVAL PTS.schedule_day DAY
                      ) as schedule_date,
                      'Complete' as status
                    FROM
                    research_response.activity_response ar 
                      left join research_response.participant_task_schedule pts on pts.participant_id = ar.participant_id
                      and pts.task_instance_id = ar.task_instance_id
                      and pts.study_version_id = ar.study_version_id
                      left join research_analytics.participant_site_country psc on psc.participant_id = pts.participant_id
                      and psc.study_id = pts.study_id
                    where
                      pts.task_type = 'activity'
                      and ar.study_id = ?
                      and psc.study_id = ?
                      ${params.siteId ? `and psc.site_id = '${params.siteId}' ` : ''}
                      ${params.participantsIds ? `and psc.participant_id in (${participantsIds}) ` : ''} 
                      ${params.fromDate ? `and compliance_date between '${params.fromDate}' and '${params.toDate}' ` : ''} 
                  ) t1
                group by
                  country_id,
                  site_id,
                  participant_id,
                  activity_name,
                  compliance_date
                order by
                  compliance_date asc
              ) A
        ) T1
        group by study_id, site_id, activity_name, month, year
        order by date asc
        `;
        console.log(`getComplianceChangeScoreDeviation query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.log(`the length of getComplianceChangeScoreDeviation is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getComplianceChangeScoreDeviation]', er)
        throw er
      }
    }

    async getComplianceInsightScore (params) { 
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        let countryIds = null;

        for (let index = 0; index < 3; index++) {
          bindingParams.push(params.studyId)
        } 
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }

        if (params.countryIds) {
          countryIds = convertArrayToString(params.countryIds);
        }

        let querySql = `
          SELECT study_id, 
          country_id, 
          site_id,
          country_name countryName,
          site_name siteName,
          activity_name,
          compliance_date,
          AVG(CASE WHEN compliance_date BETWEEN '${params.fromDate}' and '${params.toDate}'
              THEN percent_compliance end) as percent_compliance_now,
          AVG(CASE WHEN compliance_date BETWEEN DATE_ADD('${params.fromDate}', INTERVAL -7 DAY) and DATE_ADD('${params.toDate}', INTERVAL -7 DAY)
              THEN percent_compliance end) as percent_compliance_previous7day,
          (100 - AVG(CASE WHEN compliance_date BETWEEN '${params.fromDate}' and '${params.toDate}'
              THEN percent_compliance end)) as non_compliance_now,
          (100 - AVG(CASE WHEN compliance_date BETWEEN DATE_ADD('${params.fromDate}', INTERVAL -7 DAY) and DATE_ADD('${params.toDate}', INTERVAL -7 DAY)
              THEN percent_compliance end)) as non_compliance_previous7day
        FROM (
        select
        study_id,
        participant_id,
        country_id,
        country_name,
        site_id,
        site_name, 
        compliance_date,
        due_date,
        activity_name,
        COUNT(case when (status = 'Complete' AND compliance_date < due_date) then 1 end)/COUNT(case when status = 'Complete' OR status = 'Missed' then 1 end)*100 as percent_compliance
        from
          (select
          P.participant_id,
          P.site_name,
          P.site_id,
          P.study_name,
          P.study_id,
          P.country_id,
          P.country_name,
          P.task_title as activity_name,
          P.enrollment_date,
          coalesce(DATE(P.completion_date),
                  DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as compliance_date,
          DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY) as due_date, 
          CASE 
          WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
          WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
          WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
          WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
          WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
          WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
          WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
          WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          END AS status
            from
              (select
              PSC.participant_id,
              PSC.participant_start_date,
              PSC.site_name,
              PSC.site_id,
              PSC.study_name,
              PSC.study_id,
              PSC.country_id,
              PSC.country_name,
              PSC.enrollment_date,
              PTS.task_title,
              pts.created_date,
              pts.start_day,
              pts.end_day,
              pts.schedule_day,
              case 
              when pts.task_type in ('survey', 'epro') then srt.id
              when pts.task_type = 'telehealth' then ppa.id
              when pts.task_type = 'activity' then ar.id
              end as ar_id,
              case 
              when pts.task_type in ('survey', 'epro') then 'survey'
              when pts.task_type = 'telehealth' then 'visits'
              when pts.task_type = 'activity' then 'activity'
              end as response_type,
              case 
              when pts.task_type = 'telehealth' then ppa.status
              else ''
              end as visit_status,
              case 
              when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
              when pts.task_type = 'telehealth' then ppa.end_time
              when pts.task_type = 'activity' then ar.end_time
              end as completion_date
                from research_analytics.participant_site_country PSC
                RIGHT JOIN research_response.participant_task_schedule PTS ON PTS.study_id = ?
                      and PSC.participant_id = PTS.participant_id
                      and PSC.study_id = PTS.study_id
                LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                      and pts.task_instance_id = ar.task_instance_id
                      and pts.study_version_id = ar.study_version_id
                LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                      and pts.task_instance_id = srt.task_instance_id
                      and pts.study_version_id = srt.study_version_id
                LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                      and pts.task_instance_id = ppa.task_instanceuuid
                      and pts.study_version_id = ppa.study_version_id
                      and (ppa.visit_id IS NOT NULL)
                WHERE PSC.study_id = ?
                    and PTS.study_id = ?
                    ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
                    ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''}
                    ${params.countryIds ? `AND PSC.country_id in (${countryIds})` : ''}
              ) P
            WHERE coalesce(DATE(P.completion_date),
                  DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
                  ${params.fromDate ? `and coalesce(DATE(P.completion_date),
                  DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
                  ${params.fromDate ? `and P.enrollment_date BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
          ) T1
        GROUP BY study_id, participant_id, country_id, site_id, activity_name, compliance_date
        ) C
        GROUP BY study_id, country_id, site_id
        `;
        console.log(`getComplianceInsightScore query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getComplianceInsightScore Done In ${JSON.stringify(params)}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getComplianceInsightScore Done In ${JSON.stringify(params)}`);
        console.log(`the length of getComplianceInsightScore is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getComplianceInsightScore]', er)
        throw er
      }
    }

    async getNonCompliantActivities (params) { 
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        let countryIds = null;
        for (let index = 0; index < 3; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        if (params.countryIds) {
          countryIds = convertArrayToString(params.countryIds);
        }
        let querySql = `
        SELECT study_id, 
        country_name as countryName, 
        site_name as siteName,
        country_id, 
        site_id,
        activity_name as activityName,
        compliance_date,
        month(compliance_date) as month,
        year(compliance_date) as year,
        avg(percent_compliance) as percent_compliance,
        (100 - avg(percent_compliance)) as nonCompliance
        FROM (
        select
        study_id,
        participant_id,
        country_id,
        country_name,
        site_id,
        site_name, 
        compliance_date,
        due_date,
        activity_name,
        COUNT(case when (status = 'Complete' AND compliance_date < due_date) then 1 end)/COUNT(case when status = 'Complete' OR status = 'Missed' then 1 end)*100 as percent_compliance
        from
        (select
        P.participant_id,
        P.site_name,
        P.site_id,
        P.study_name,
        P.study_id,
        P.country_id,
        P.country_name,
        P.task_title as activity_name,
        P.enrollment_date,
        coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as compliance_date,
        DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY) as due_date, 
        CASE 
        WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
        WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
        WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
        WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
        WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
        WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
        WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
        WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
        WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
        END AS status
          from
            (select
            PSC.participant_id,
            PSC.participant_start_date,
            PSC.site_name,
            PSC.site_id,
            PSC.study_name,
            PSC.study_id,
            PSC.country_id,
            PSC.country_name,
            PSC.enrollment_date,
            PTS.task_title,
            pts.created_date,
            pts.start_day,
            pts.end_day,
            pts.schedule_day,
            case 
            when pts.task_type in ('survey', 'epro') then srt.id
            when pts.task_type = 'telehealth' then ppa.id
            when pts.task_type = 'activity' then ar.id
            end as ar_id,
            case 
            when pts.task_type in ('survey', 'epro') then 'survey'
            when pts.task_type = 'telehealth' then 'visits'
            when pts.task_type = 'activity' then 'activity'
            end as response_type,
            case 
            when pts.task_type = 'telehealth' then ppa.status
            else ''
            end as visit_status,
            case 
            when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
            when pts.task_type = 'telehealth' then ppa.end_time
            when pts.task_type = 'activity' then ar.end_time
            end as completion_date
              from research_analytics.participant_site_country PSC
              RIGHT JOIN research_response.participant_task_schedule PTS ON PTS.study_id = ?
                    and PSC.participant_id = PTS.participant_id
                    and PSC.study_id = PTS.study_id
              LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                    and pts.task_instance_id = ar.task_instance_id
                    and pts.study_version_id = ar.study_version_id
              LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                    and pts.task_instance_id = srt.task_instance_id
                    and pts.study_version_id = srt.study_version_id
              LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                    and pts.task_instance_id = ppa.task_instanceuuid
                    and pts.study_version_id = ppa.study_version_id
                    and (ppa.visit_id IS NOT NULL)
              WHERE PSC.study_id = ? 
                  and PTS.study_id = ?
                  ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''} 
                  ${params.countryIds ? `and PSC.country_id in (${countryIds}) ` : ''} 
                  ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
            ) P
          WHERE coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
                ${params.fromDate ? `and coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
                ${params.fromDate ? `and P.enrollment_date BETWEEN '${params.fromDate}' and '${params.toDate}'` : ''}
          ) T1
          GROUP BY study_id, participant_id, country_id, site_id, activity_name, compliance_date
          ) C
          GROUP BY study_id, activity_name, country_id, site_id
          ORDER BY nonCompliance desc
          LIMIT 10
        `;
        console.log(`getNonCompliantActivities query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.log(`the length of getNonCompliantActivities is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getNonCompliantActivities]', er)
        throw er
      }
    }

    //BELOW FOR COMPLETION
    async getCompletion (params) { 
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        for (let index = 0; index < 5; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select 
          study_id,
          country_id,
          country_name as countryName,
          site_id,
          site_name as siteName,
          year,
          month,
          completion_date as date,
          GROUP_CONCAT(case when percent_completion between 0 and 0.99 then activityName end SEPARATOR '||') as "completion-0",
          GROUP_CONCAT(case when percent_completion between 1 and 9.99 then activityName end SEPARATOR '||') as "completion-1-10",
          GROUP_CONCAT(case when percent_completion = 10.00 then activityName end SEPARATOR '||') as "completion-10",
          GROUP_CONCAT(case when percent_completion between 10.01 and 19.99 then activityName end SEPARATOR '||') as "completion-10-20",
          GROUP_CONCAT(case when percent_completion = 20.00 then activityName end SEPARATOR '||') as "completion-20",
          GROUP_CONCAT(case when percent_completion between 20.01 and 29.99 then activityName end SEPARATOR '||') as "completion-20-30",
          GROUP_CONCAT(case when percent_completion = 30.00 then activityName end SEPARATOR '||') as "completion-30",
          GROUP_CONCAT(case when percent_completion between 30.01 and 39.99 then activityName end SEPARATOR '||') as "completion-30-40",
          GROUP_CONCAT(case when percent_completion = 40.00 then activityName end SEPARATOR '||') as "completion-40",
          GROUP_CONCAT(case when percent_completion between 40.01 and 49.99 then activityName end SEPARATOR '||') as "completion-40-50",
          GROUP_CONCAT(case when percent_completion = 50.00 then activityName end SEPARATOR '||') as "completion-50",
          GROUP_CONCAT(case when percent_completion between 50.01 and 59.99 then activityName end SEPARATOR '||') as "completion-50-60",
          GROUP_CONCAT(case when percent_completion = 60.00 then activityName end SEPARATOR '||') as "completion-60",
          GROUP_CONCAT(case when percent_completion between 60.01 and 69.99 then activityName end SEPARATOR '||') as "completion-60-70",
          GROUP_CONCAT(case when percent_completion = 70.00 then activityName end SEPARATOR '||') as "completion-70",
          GROUP_CONCAT(case when percent_completion between 70.01 and 79.99 then activityName end SEPARATOR '||') as "completion-70-80",
          GROUP_CONCAT(case when percent_completion = 80.00 then activityName end SEPARATOR '||') as "completion-80",
          GROUP_CONCAT(case when percent_completion between 80.01 and 89.99 then activityName end SEPARATOR '||') as "completion-80-90",
          GROUP_CONCAT(case when percent_completion = 90.00 then activityName end SEPARATOR '||') as "completion-90",
          GROUP_CONCAT(case when percent_completion between 90.01 and 99.99 then activityName end SEPARATOR '||') as "completion-90-100",
          GROUP_CONCAT(case when percent_completion = 100.00 then activityName end SEPARATOR '||') as "completion-100"
      FROM (
      SELECT 
          study_id, concat(activity_name, '**', count(participant_id), '::', round(avg(percent_completion), 0), '++', completion_date) as activityName,
              country_id, country_name, site_id, site_name, 
          EXTRACT(YEAR from completion_date) as year,
          EXTRACT(MONTH from completion_date) as month,
          avg(percent_completion) as percent_completion,
          completion_date
          FROM (SELECT *
              FROM (SELECT * FROM (
      select
          study_id,
          participant_id,
          country_id,
          country_name,
          site_id,
          site_name,
          completion_date,
          activity_name,
          IFNULL(COUNT(case when status = 'complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
          FROM
          (SELECT
              O.participant_id, 
              O.study_id,
              PSC.country_id,
              PSC.country_name,
              PSC.site_id,
              PSC.site_name,
              case 
                  when P.participant_id is not null then 'complete'
                  when P.participant_id is null then 'incomplete'    
              end as status,
              completion_date,
              'Econsent Completion' as activity_name
              FROM
              (select 
                  participant_id,
                  study_id,
                  count(*) as attempts
                  FROM research_response.on_boarding_response 
                  where type = 'econsent' and study_id = ? and LENGTH(participant_id) > 3
                  group by participant_id 
                  order by attempts desc) O
              LEFT JOIN 
              (select 
                  distinct(participant_id) as participant_id,
                  study_id,
                  doc_id,
                  DATE(doc_completion_date) as completion_date 
                  FROM 
                      (select 
                          PP.doc_id,
                          PP.participant_id,
                          PP.study_id,
                          PP.remarks,
                          PP.document,
                          PP.doc_completion_date
                          FROM 
                          (select
                              Q.participant_id,
                              Q.study_id,
                              Q.doc_id,
                              Q.site_id,
                              case 
                                  when Q.uploaded_time is not null then 'uploaded'
                                  when Q.status = 'SignedByParticipant' then 'Self'
                                  when Q.status = 'SignedByPI' then 'Dual'   
                                  end as remarks,    
                              case
                                  when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                                  when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'
                                  end as document,  
                              case
                                  when Q.doc_completion_date LIKE ('%UTC%') then STR_TO_DATE(doc_completion_date, '%a %b %d %T UTC %Y')
                                  else Q.doc_completion_date
                                  end as doc_completion_date
                                  FROM
                                  (select *,
                                      max(created_time) as doc_completion_date
                                      FROM research.participant_study_document PSD
                                      WHERE PSD.participant_id is not null
                                        and PSD.study_id = ?
                                        ${params.participantsIds ? `and PSD.participant_id in (${participantsIds}) ` : ''}
                                        ${params.siteId ? `and PSD.site_id = '${params.siteId}' ` : ''}
                                      GROUP BY participant_id, created_time) Q
                                      WHERE Q.participant_id is not null) PP) PQ
                                  where PQ.document = 'econsent' ) P 
                  ON O.participant_id = P.participant_id and O.study_id = P.study_id
              INNER JOIN research_analytics.participant_site_country PSC 
                  ON O.participant_id = PSC.participant_id and O.study_id = PSC.study_id
              WHERE completion_date <= CURDATE()
          ) ECS
      GROUP BY participant_id, country_id, site_id, activity_name, completion_date
      ) a
              UNION SELECT * FROM (
      SELECT
          study_id,
          participant_id,
          country_id,
          country_name,
          site_id,
          site_name,
          completion_date,
          activity_name,
          IFNULL(
              SUM(
                  case 
                      when remarks = 'uploaded' then 1 
                      when remarks = 'Dual' then 1 end)/COUNT(remarks)*100, 0)
                      as percent_completion
          FROM
          (SELECT 
              PP.doc_id,
              PP.doc_name as activity_name,
              PP.participant_id, 
              PP.study_id,
              PP.remarks,
              PP.document,
              DATE(PP.completion_date) as completion_date,
              PSC.country_id,
              PSC.country_name,
              PSC.site_id,
              PSC.site_name
              FROM 
              (select
                  Q.participant_id,
                  Q.study_id,
                  Q.doc_id, 
                  Q.doc_name,
                  Q.completion_date,
                  case 
                      when Q.uploaded_time is not null then 'uploaded'
                      when Q.status = 'SignedByParticipant' then 'Self'
                      when Q.status = 'SignedByPI' then 'Dual'
                      end as remarks, 
                  case
                      when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                      when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'   
                      end as document
                  from (
                  SELECT *, 
                      max(created_time) as completion_date 
                  FROM research.participant_study_document  P
                  WHERE P.participant_id is not null 
                      and P.study_id = ? 
                      ${params.participantsIds ? `and P.participant_id in (${participantsIds}) ` : ''}
                      ${params.siteId ? `and P.site_id = '${params.siteId}' ` : ''}
                  GROUP BY participant_id, created_time)  Q
                  WHERE Q.participant_id is not null
              ) PP 
          INNER JOIN research_analytics.participant_site_country PSC 
              ON PP.participant_id = PSC.participant_id and PP.study_id = PSC.study_id
          ) PQ
      WHERE completion_date <= CURDATE()
      GROUP BY participant_id, country_id, site_id, activity_name, completion_date
      ) b 
              UNION SELECT * FROM (
      select
      study_id,
      participant_id,
      country_id,
      country_name,
      site_id,
      site_name, 
      completion_date,
      activity_name,
      IFNULL(COUNT(case when status = 'Complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
      from
        (select
        P.participant_id,
        P.site_name,
        P.site_id,
        P.study_name,
        P.study_id,
        P.country_id,
        P.country_name,
        P.task_title as activity_name,
        coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as completion_date,
        CASE 
        WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
        WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
        WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
        WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
        WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
        WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
        WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
        WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
        WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
        END AS status
          from
            (select
            PSC.participant_id,
            PSC.participant_start_date,
            PSC.site_name,
            PSC.site_id,
            PSC.study_name,
            PSC.study_id,
            PSC.country_id,
            PSC.country_name,
            PTS.task_title,
            pts.created_date,
            pts.start_day,
            pts.end_day,
            case 
            when pts.task_type in ('survey', 'epro') then srt.id
            when pts.task_type = 'telehealth' then ppa.id
            when pts.task_type = 'activity' then ar.id
            end as ar_id,
            case 
            when pts.task_type in ('survey', 'epro') then 'survey'
            when pts.task_type = 'telehealth' then 'visits'
            when pts.task_type = 'activity' then 'activity'
            end as response_type,
            case 
            when pts.task_type = 'telehealth' then ppa.status
            else ''
            end as visit_status,
            case 
            when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
            when pts.task_type = 'telehealth' then ppa.end_time
            when pts.task_type = 'activity' then ar.end_time
            end as completion_date
              from research_analytics.participant_site_country  PSC
              INNER JOIN research_response.participant_task_schedule PTS ON PTS.study_id = ? and  PSC.participant_id = PTS.participant_id
                    and PSC.study_id = PTS.study_id
              LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                    and pts.task_instance_id = ar.task_instance_id
                    and pts.study_version_id = ar.study_version_id
              LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                    and pts.task_instance_id = srt.task_instance_id
                    and pts.study_version_id = srt.study_version_id
              LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                    and pts.task_instance_id = ppa.task_instanceuuid
                    and pts.study_version_id = ppa.study_version_id
                    and (ppa.visit_id IS NOT NULL)
                    
                               where PSC.study_id = ?
                          ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
                          ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''}
            ) P
          WHERE coalesce(DATE(P.completion_date),
                DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
        ) T1
          GROUP BY participant_id, country_id, site_id, activity_name, completion_date
      ) c)d)e
      GROUP BY activity_name, country_id, site_id, year, month
      ) A1
      GROUP BY country_id, site_id, year, month
      ORDER BY year, month asc
        `;
        console.log(`getCompletion query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getCompletion query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        console.timeEnd(`getCompletion query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length of getCompletion is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getCompletion]', er)
        throw er
      }
    }
    
    async getCompletionByParticipant (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let participantsIds = null;
        for (let index = 0; index < 5; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select 
        study_id, country_id, country_name as countryName, site_id, site_name as siteName,
        year,month, completion_date as date,

        COUNT(case when percent_completion between 0 and 0.99 then 1 end) as "completion-0",
        COUNT(case when percent_completion between 1 and 9.99 then 1 end) as "completion-1-10",
        COUNT(case when percent_completion = 10.00 then 1 end) as "completion-10",
        COUNT(case when percent_completion between 10.01 and 19.99 then 1 end) as "completion-10-20",
        COUNT(case when percent_completion = 20.00 then 1 end) as "completion-20",
        COUNT(case when percent_completion between 20.01 and 29.99 then 1 end) as "completion-20-30",
        COUNT(case when percent_completion = 30.00 then 1 end) as "completion-30",
        COUNT(case when percent_completion between 30.01 and 39.99 then 1 end) as "completion-30-40",
        COUNT(case when percent_completion = 40.00 then 1 end) as "completion-40",
        COUNT(case when percent_completion between 40.01 and 49.99 then 1 end) as "completion-40-50",
        COUNT(case when percent_completion = 50.00 then 1 end) as "completion-50",
        COUNT(case when percent_completion between 50.01 and 59.99 then 1 end) as "completion-50-60",
        COUNT(case when percent_completion = 60.00 then 1 end) as "completion-60",
        COUNT(case when percent_completion between 60.01 and 69.99 then 1 end) as "completion-60-70",
        COUNT(case when percent_completion = 70.00 then 1 end) as "completion-70",
        COUNT(case when percent_completion between 70.01 and 79.99 then 1 end) as "completion-70-80",
        COUNT(case when percent_completion = 80.00 then 1 end) as "completion-80",
        COUNT(case when percent_completion between 80.01 and 89.99 then 1 end) as "completion-80-90",
        COUNT(case when percent_completion = 90.00 then 1 end) as "completion-90",
        COUNT(case when percent_completion between 90.01 and 99.99 then 1 end) as "completion-90-100",
        COUNT(case when percent_completion = 100.00 then 1 end) as "completion-100"
        FROM 
        (
        select 
        study_id, participant_id, country_id, country_name, site_id, site_name, 
        EXTRACT(YEAR from completion_date) as year,
        EXTRACT(MONTH from completion_date) as month,
        FLOOR(avg(percent_completion)) as percent_completion,
        completion_date
        FROM (SELECT *
                FROM (SELECT * FROM (
        select
            study_id,
            participant_id,
            country_id,
            country_name,
            site_id,
            site_name,
            completion_date,
            activity_name,
            IFNULL(COUNT(case when status = 'complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
            FROM
            (SELECT
                O.participant_id, 
                O.study_id,
                PSC.country_id,
                PSC.country_name,
                PSC.site_id,
                PSC.site_name,
                case 
                    when P.participant_id is not null then 'complete'
                    when P.participant_id is null then 'incomplete'    
                end as status,
                completion_date,
                'Econsent Completion' as activity_name
                FROM
                (select 
                    participant_id,
                    study_id,
                    count(*) as attempts
                    FROM research_response.on_boarding_response 
                    where type = 'econsent' and study_id = ? and LENGTH(participant_id) > 3
                    group by participant_id 
                    order by attempts desc) O
                LEFT JOIN 
                (select 
                    distinct(participant_id) as participant_id,
                    study_id,
                    doc_id,
                    DATE(doc_completion_date) as completion_date 
                    FROM 
                        (select 
                            PP.doc_id,
                            PP.participant_id,
                            PP.study_id,
                            PP.remarks,
                            PP.document,
                            PP.doc_completion_date
                            FROM 
                            (select
                                Q.participant_id,
                                Q.study_id,
                                Q.doc_id,
                                Q.site_id,
                                case 
                                    when Q.uploaded_time is not null then 'uploaded'
                                    when Q.status = 'SignedByParticipant' then 'Self'
                                    when Q.status = 'SignedByPI' then 'Dual'   
                                    end as remarks,    
                                case
                                    when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                                    when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'
                                    end as document,  
                                case
                                    when Q.doc_completion_date LIKE ('%UTC%') then STR_TO_DATE(doc_completion_date, '%a %b %d %T UTC %Y')
                                    else Q.doc_completion_date
                                    end as doc_completion_date
                                    FROM
                                    (select *,
                                        max(created_time) as doc_completion_date
                                        FROM research.participant_study_document PSD
                                        WHERE PSD.participant_id is not null
                                          and PSD.study_id = ?
                                          ${params.participantsIds ? `and PSD.participant_id in (${participantsIds}) ` : ''}
                                          ${params.siteId ? `and PSD.site_id = '${params.siteId}' ` : ''}
                                        GROUP BY participant_id, created_time) Q
                                        WHERE Q.participant_id is not null) PP) PQ
                                    where PQ.document = 'econsent' ) P 
                    ON O.participant_id = P.participant_id and O.study_id = P.study_id
                INNER JOIN research_analytics.participant_site_country PSC 
                    ON O.participant_id = PSC.participant_id and O.study_id = PSC.study_id
                WHERE completion_date <= CURDATE()
            ) ECS
        GROUP BY participant_id, country_id, site_id, activity_name, completion_date
        ) a
                UNION SELECT * FROM (
        SELECT
            study_id,
            participant_id,
            country_id,
            country_name,
            site_id,
            site_name,
            completion_date,
            activity_name,
            IFNULL(
                SUM(
                    case 
                        when remarks = 'uploaded' then 1 
                        when remarks = 'Dual' then 1 end)/COUNT(remarks)*100, 0)
                        as percent_completion
            FROM
            (SELECT 
                PP.doc_id,
                PP.doc_name as activity_name,
                PP.participant_id, 
                PP.study_id,
                PP.remarks,
                PP.document,
                DATE(PP.completion_date) as completion_date,
                PSC.country_id,
                PSC.country_name,
                PSC.site_id,
                PSC.site_name
                FROM 
                (select
                    Q.participant_id,
                    Q.study_id,
                    Q.doc_id, 
                    Q.doc_name,
                    Q.completion_date,
                    case 
                        when Q.uploaded_time is not null then 'uploaded'
                        when Q.status = 'SignedByParticipant' then 'Self'
                        when Q.status = 'SignedByPI' then 'Dual'
                        end as remarks, 
                    case
                        when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                        when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'   
                        end as document
                    from (
                    SELECT *, 
                        max(created_time) as completion_date 
                    FROM research.participant_study_document  P
                    WHERE P.participant_id is not null 
                        and P.study_id = ? 
                        ${params.participantsIds ? `and P.participant_id in (${participantsIds}) ` : ''}
                        ${params.siteId ? `and P.site_id = '${params.siteId}' ` : ''}
                    GROUP BY participant_id, created_time)  Q
                    WHERE Q.participant_id is not null
                ) PP 
            INNER JOIN research_analytics.participant_site_country PSC 
                ON PP.participant_id = PSC.participant_id and PP.study_id = PSC.study_id
            ) PQ
        WHERE completion_date <= CURDATE()
        GROUP BY participant_id, country_id, site_id, activity_name, completion_date
        ) b 
                UNION SELECT * FROM (
        select
        study_id,
        participant_id,
        country_id,
        country_name,
        site_id,
        site_name, 
        completion_date,
        activity_name,
        IFNULL(COUNT(case when status = 'Complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
        from
          (select
          P.participant_id,
          P.site_name,
          P.site_id,
          P.study_name,
          P.study_id,
          P.country_id,
          P.country_name,
          P.task_title as activity_name,
          coalesce(DATE(P.completion_date),
          DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as completion_date,
          P.enrollment_date,
          CASE 
            WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
            WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
            WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
            WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
            WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
            WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
            WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
            WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
            WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          END AS status
            from
              (select
              PSC.participant_id,
              PSC.participant_start_date,
              PSC.site_name,
              PSC.site_id,
              PSC.study_name,
              PSC.study_id,
              PSC.country_id,
              PSC.country_name,
              PSC.enrollment_date,
              PTS.task_title,
              pts.created_date,
              pts.start_day,
              pts.end_day,
              case 
              when pts.task_type in ('survey', 'epro') then srt.id
              when pts.task_type = 'telehealth' then ppa.id
              when pts.task_type = 'activity' then ar.id
              end as ar_id,
              case 
              when pts.task_type in ('survey', 'epro') then 'survey'
              when pts.task_type = 'telehealth' then 'visits'
              when pts.task_type = 'activity' then 'activity'
              end as response_type,
              case 
              when pts.task_type = 'telehealth' then ppa.status
              else ''
              end as visit_status,
              case 
              when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
              when pts.task_type = 'telehealth' then ppa.end_time
              when pts.task_type = 'activity' then ar.end_time
              end as completion_date
                from research_analytics.participant_site_country PSC
                INNER JOIN research_response.participant_task_schedule PTS ON PTS.study_id = ? and  PSC.participant_id = PTS.participant_id
                      and PSC.study_id = PTS.study_id
                LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                      and pts.task_instance_id = ar.task_instance_id
                      and pts.study_version_id = ar.study_version_id
                LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                      and pts.task_instance_id = srt.task_instance_id
                      and pts.study_version_id = srt.study_version_id
                LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                      and pts.task_instance_id = ppa.task_instanceuuid
                      and pts.study_version_id = ppa.study_version_id
                      and (ppa.visit_id IS NOT NULL)
                                 where PSC.study_id = ?
                          ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''} 
                          ${params.participantsIds ? `and PSC.participant_id in (${participantsIds}) ` : ''}
              ) P
            WHERE coalesce(DATE(P.completion_date),
                  DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
          ) T1
            GROUP BY participant_id, country_id, site_id, activity_name, completion_date
        ) c)d)e
        GROUP BY participant_id, country_id, site_id, year, month
        ) A1
        GROUP BY country_id, site_id, year, month
        ORDER BY year, month asc
        `;
        console.log(`getCompletionByParticipant query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getCompletionByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getCompletionByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`The length of getCompletionByParticipant is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getCompletionByParticipant]', er)
        throw er
      }
    }
    
    async getCompletionDetails (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let siteIds = null;
        let participantsIds = null;
        for (let index = 0; index < 11; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.siteIds) {
          siteIds = convertArrayToString(params.siteIds);
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select 
        study_id, activity_name as activityName, country_id, country_name, site_id, site_name,
        max(completion_date) as complianceDate, 
        avg(percent_completion) as percentCompliance
        FROM 
        (
        select 
        *
        from
        (
        select * from
        (
        select
        study_id, participant_id, country_id, country_name, site_id, site_name,
        completion_date,activity_name,
        IFNULL(COUNT(case when status = 'complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
        FROM
        (SELECT
        O.participant_id, 
        O.study_id,
        PSC.country_id,
        PSC.country_name,
        PSC.site_id,
        PSC.site_name,
        case 
            when P.participant_id is not null then 'complete'
            when P.participant_id is null then 'incomplete'    
        end as status,
        DATE(P.doc_completion_date) as completion_date,
        'Econsent Completion' as activity_name
        FROM
        (select 
        participant_id,
        study_id,
        count(*) as attempts
        FROM research_response.on_boarding_response 
        where type = 'econsent' and LENGTH(participant_id) > 3
        group by participant_id 
        order by attempts desc) O
        LEFT JOIN 
        (select distinct(participant_id) as participant_id,
        study_id,
        doc_id,
        doc_completion_date 
        FROM 
        (select PP.doc_id,
        PP.participant_id,
        PP.study_id,
        PP.remarks,
        PP.document,
        PP.doc_completion_date
        FROM 
        (select
        Q.participant_id,
        Q.study_id,
        Q.doc_id,
        Q.site_id,
        case 
            when Q.uploaded_time is not null then 'uploaded'
            when Q.status = 'SignedByParticipant' then 'Self'
            when Q.status = 'SignedByPI' then 'Dual'   
            end as remarks,    
        case
            when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
            when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'
            end as document,  
        case
            when Q.doc_completion_date LIKE ('%UTC%') then STR_TO_DATE(doc_completion_date, '%a %b %d %T UTC %Y')
            else Q.doc_completion_date
            end as doc_completion_date
        FROM
        (select *,
        max(created_time) as doc_completion_date
        from research.participant_study_document PSD
        where PSD.participant_id is not null 
        group by participant_id, created_time) Q
        where Q.participant_id is not null)
        PP) PQ
        where PQ.document = 'econsent' ) P ON O.participant_id = P.participant_id and O.study_id = P.study_id
        INNER JOIN (
        select * 
        from
        research_analytics.participant_site_country
        where study_id = ?
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ) PSC ON O.participant_id = PSC.participant_id and O.study_id = PSC.study_id
        ) ECS
        WHERE study_id = ? 
        ${params.fromDate ? `and completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''} 
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ${params.participantsIds ? ` and participant_id in (${participantsIds}) ` : ''}
            
        GROUP by country_id, site_id, participant_id, activity_name, completion_date
        order by completion_date asc
        ) T1
        UNION
        select * from
        (
        SELECT
        study_id, participant_id, country_id, country_name, site_id, site_name,
        completion_date, doc_name as activity_name,
        IFNULL(SUM(case when remarks = 'uploaded' then 1 
                  when remarks = 'Dual' then 1 end)/COUNT(remarks)*100, 0) as percent_completion
        FROM
        (select 
        PP.doc_id,
        PP.doc_name,
        PP.participant_id, 
        PP.study_id,
        PP.remarks,
        PP.document,
        DATE(PP.completion_date) as completion_date,
        PSC.country_id,
        PSC.country_name,
        PSC.site_id,
        PSC.site_name
        from 
        (select
        Q.participant_id,
        Q.study_id,
        Q.doc_id, 
        Q.doc_name,
        Q.completion_date,
        case 
                when Q.uploaded_time is not null then 'uploaded'
                when Q.status = 'SignedByParticipant' then 'Self'
                when Q.status = 'SignedByPI' then 'Dual'
                
            end as remarks,
            
        case
                when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'
                
            end as document
        from (
        select *, 
        max(created_time) as completion_date 
        from research.participant_study_document  P
        where P.participant_id is not null 
        group by participant_id, created_time)  Q
        where Q.participant_id is not null) PP 
        INNER JOIN (
        select * 
        from
        research_analytics.participant_site_country
        where study_id = ?
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ) PSC ON PP.participant_id = PSC.participant_id and PP.study_id = PSC.study_id 
        ) PQ
        WHERE study_id = ?
        ${params.fromDate ? `and completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''}
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
        GROUP BY country_id, site_id, participant_id, activity_name, completion_date
        order by completion_date asc
        ) T3
        UNION
        select * from
        (
        select
        study_id, participant_id, country_id, country_name, site_id, site_name, 
        completion_date,activity_name,
        IFNULL(COUNT(case when status = 'Complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
        from
        (select 
        PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
        PTS.task_title as activity_name,
        coalesce(DATE(PTS.end_time), DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) as completion_date,
        DATE_ADD(PSC.participant_start_date, INTERVAL PTS.schedule_day DAY) as schedule_date, 
        PTS.end_day, PTS.schedule_day,
        CASE
        WHEN PTS.response_type = 'visits' AND PTS.ar_id IS NOT NULL AND PTS.visit_status != 'Reschedule' THEN 'Complete'
        WHEN PTS.response_type = 'visits' AND PTS.ar_id IS NOT NULL AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status
        WHEN PTS.response_type = 'visits' AND PTS.end_day = (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND end_day - start_day >= 1  THEN 'Missed' 
        WHEN PTS.response_type = 'visits' AND PTS.end_day < (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.ar_id IS NULL THEN 'Missed'   
        WHEN PTS.response_type = 'visits' AND PTS.end_day >= (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) THEN 'Scheduled' 
        WHEN PTS.response_type != 'visits' AND PTS.ar_id IS NOT NULL THEN 'Complete' 
        WHEN PTS.response_type != 'visits' AND PTS.end_day = (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.end_day - PTS.start_day >= 1  THEN 'Missed' 
        WHEN PTS.response_type != 'visits' AND PTS.end_day < (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.ar_id IS NULL THEN 'Missed'   
        WHEN PTS.response_type != 'visits' AND PTS.end_day >= (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) THEN 'Scheduled'   
        END AS status 
        from
        (
        select * 
        from
        research_analytics.participant_site_country
        where study_id = ?
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ) PSC
        INNER JOIN (
        select
        pts.participant_id,
        PTS.study_id,
        pts.task_title, 
        pts.start_day, 
        ar.end_time,
        pts.end_day, 
        pts.schedule_day, 
        ar.id ar_id,
        'activity' as response_type,
        '' as visit_status
        FROM research_response.participant_task_schedule  pts 
        left join 
        research_response.activity_response ar on  pts.participant_id = ar.participant_id and pts.task_instance_id = ar.task_instance_id and pts.study_version_id = ar.study_version_id 
        where pts.task_type IN ('activity') and pts.study_id = ? ${params.participantsIds ? `and pts.participant_id in (${participantsIds}) ` : ''}
        ) PTS ON PSC.participant_id = PTS.participant_id and
                                                                      PSC.study_id = PTS.study_id  
        ) t1
        ${params.siteIds || params.fromDate ? 'WHERE ' : ''}
        ${params.fromDate ? `completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''} ${params.fromDate && params.siteId ? 'AND ' : ''}
        ${params.siteIds ? ` site_id in (${siteIds}) ` : ''}
        GROUP BY country_id, site_id, participant_id, activity_name, completion_date
        ORDER BY completion_date ASC
        ) T3
        UNION
        select * from
        (
        select
        study_id, participant_id, country_id, country_name, site_id, site_name, 
        completion_date,activity_name,
        IFNULL(COUNT(case when status = 'Complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
        from
        (select 
        PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
        PTS.task_title as activity_name,
        coalesce(DATE(PTS.end_time), DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) as completion_date,
        DATE_ADD(PSC.participant_start_date, INTERVAL PTS.schedule_day DAY) as schedule_date, 
        PTS.end_day, PTS.schedule_day,
        CASE
        WHEN PTS.response_type = 'visits' AND PTS.ar_id IS NOT NULL AND PTS.visit_status != 'Reschedule' THEN 'Complete'
        WHEN PTS.response_type = 'visits' AND PTS.ar_id IS NOT NULL AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status
        WHEN PTS.response_type = 'visits' AND PTS.end_day = (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND end_day - start_day >= 1  THEN 'Missed' 
        WHEN PTS.response_type = 'visits' AND PTS.end_day < (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.ar_id IS NULL THEN 'Missed'   
        WHEN PTS.response_type = 'visits' AND PTS.end_day >= (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) THEN 'Scheduled' 
        WHEN PTS.response_type != 'visits' AND PTS.ar_id IS NOT NULL THEN 'Complete' 
        WHEN PTS.response_type != 'visits' AND PTS.end_day = (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.end_day - PTS.start_day >= 1  THEN 'Missed' 
        WHEN PTS.response_type != 'visits' AND PTS.end_day < (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.ar_id IS NULL THEN 'Missed'   
        WHEN PTS.response_type != 'visits' AND PTS.end_day >= (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) THEN 'Scheduled'   
        END AS status 
        from
        (
        select * 
        from
        research_analytics.participant_site_country
        where study_id = ?
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ) PSC
        INNER JOIN (
        SELECT  
        pts.participant_id,
        PTS.study_id,
        pts.task_title,
        pts.start_day,
        pts.end_day,
        srt.completion_time_utc as end_time,
        pts.schedule_day,
        srt.id as ar_id,
        'survey' as response_type,
        '' as visit_status
        FROM research_response.participant_task_schedule pts
        left join
        research_response.survey_response_tracker srt on  pts.participant_id = srt.participant_id and pts.task_instance_id = srt.task_instance_id  and pts.study_version_id = srt.study_version_id 
        where pts.task_type IN ('survey', 'epro') and pts.study_id = ? ${params.participantsIds ? `and pts.participant_id in (${participantsIds}) ` : ''}
        ) PTS ON PSC.participant_id = PTS.participant_id and
                                                                      PSC.study_id = PTS.study_id 
        ) t1
        ${params.siteIds || params.fromDate ? 'WHERE ' : ''}
        ${params.fromDate ? `completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''} ${params.fromDate && params.siteIds ? 'AND ' : ''}
        ${params.siteIds ? ` site_id in (${siteIds}) ` : ''}
        GROUP BY country_id, site_id, participant_id, activity_name, completion_date
        ORDER BY completion_date asc
        ) T4
        UNION 
        select * from
        (
        select
        study_id, participant_id, country_id, country_name, site_id, site_name, 
        completion_date,activity_name,
        IFNULL(COUNT(case when status = 'Complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
        from
        (select 
        PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
        'Participant Telehealth' as activity_name,
        coalesce(DATE(PTS.end_time), DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) as completion_date,
        DATE_ADD(PSC.participant_start_date, INTERVAL PTS.schedule_day DAY) as schedule_date, 
        PTS.end_day, PTS.schedule_day,
        CASE
        WHEN PTS.response_type = 'visits' AND PTS.ar_id IS NOT NULL AND PTS.visit_status != 'Reschedule' THEN 'Complete'
        WHEN PTS.response_type = 'visits' AND PTS.ar_id IS NOT NULL AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status
        WHEN PTS.response_type = 'visits' AND PTS.end_day = (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND end_day - start_day >= 1  THEN 'Missed' 
        WHEN PTS.response_type = 'visits' AND PTS.end_day < (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.ar_id IS NULL THEN 'Missed'   
        WHEN PTS.response_type = 'visits' AND PTS.end_day >= (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) THEN 'Scheduled' 
        WHEN PTS.response_type != 'visits' AND PTS.ar_id IS NOT NULL THEN 'Complete' 
        WHEN PTS.response_type != 'visits' AND PTS.end_day = (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.end_day - PTS.start_day >= 1  THEN 'Missed' 
        WHEN PTS.response_type != 'visits' AND PTS.end_day < (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) AND PTS.ar_id IS NULL THEN 'Missed'   
        WHEN PTS.response_type != 'visits' AND PTS.end_day >= (TIMESTAMPDIFF(day,PSC.participant_start_date, NOW())) THEN 'Scheduled'   
        END AS status     
        from
        (
        select * 
        from
        research_analytics.participant_site_country
        where study_id = ?
        ${params.siteIds ? `and site_id in (${siteIds}) ` : ''}
        ) PSC
        INNER JOIN (
        select
        pts.participant_id,
        PTS.study_id,
        pts.task_title,
        pts.start_day,
        pts.end_day,
        pts.schedule_day,
        ppa.end_time,
        ppa.id ar_id,
        'visits' as response_type,
        ppa.visit_id,
        ppa.status as visit_status
        FROM research_response.participant_task_schedule pts
        left join research_response.pi_participant_appointment ppa on  pts.participant_id = ppa.participant_id and pts.task_instance_id = ppa.task_instanceuuid and pts.study_version_id = ppa.study_version_id 
        WHERE pts.task_type IN ('telehealth') and ppa.visit_id is not null and pts.study_id = ? ${params.participantsIds ? `and pts.participant_id in (${participantsIds}) ` : ''}
        ) PTS ON PSC.participant_id = PTS.participant_id and
                                                                      PSC.study_id = PTS.study_id
        ) t1
        ${params.siteIds || params.fromDate ? 'WHERE ' : ''}
        ${params.fromDate ? `completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''} ${params.fromDate && params.siteIds ? 'AND ' : ''}
        ${params.siteIds ? ` site_id in (${siteIds}) ` : ''}
        group by country_id, site_id, participant_id, activity_name, completion_date
        order by completion_date asc
        ) T5
        ) F1
        ) A1
        where completion_date is not null
        group by activity_name
        order by completion_date asc
        `;
        console.log(`getCompletionDetails query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getCompletionDetails query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getCompletionDetails query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length of getCompletionDetails is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getCompletionDetails]', er)
        throw er
      }
    }

    async getCompletionDetailsByParticipant (params) { // there's params.groupBy
      const {studyId, siteNames, countryNames} = params;
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        let siteIds = null;
        let participantsIds = null;
        for (let index = 0; index < 11; index++) {
          bindingParams.push(params.studyId)
        }
        if (params.siteIds) {
          siteIds = convertArrayToString(params.siteIds);
        }
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select 
        study_id,
        participant_id as participantId,
        country_id,
        country_name,
        site_id,
        site_name,
        max(completion_date) as complianceDate,
        FLOOR(avg(percent_completion)) as percentCompliance
        FROM 
        (
        select 
        study_id, participant_id, country_id, country_name, site_id, site_name, 
        avg(percent_completion) as percent_completion,
        completion_date
        FROM (SELECT *
                FROM (SELECT * FROM (
        select
            study_id,
            participant_id,
            country_id,
            country_name,
            site_id,
            site_name,
            completion_date,
            activity_name,
            IFNULL(COUNT(case when status = 'complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
            FROM
            (SELECT
                O.participant_id, 
                O.study_id,
                PSC.country_id,
                PSC.country_name,
                PSC.site_id,
                PSC.site_name,
                case 
                    when P.participant_id is not null then 'complete'
                    when P.participant_id is null then 'incomplete'    
                end as status,
                completion_date,
                'Econsent Completion' as activity_name
                FROM
                (select 
                    participant_id,
                    study_id,
                    count(*) as attempts
                    FROM research_response.on_boarding_response 
                    where type = 'econsent' and study_id = ? and LENGTH(participant_id) > 3
                    group by participant_id 
                    order by attempts desc) O
                LEFT JOIN 
                (select 
                    distinct(participant_id) as participant_id,
                    study_id,
                    doc_id,
                    DATE(doc_completion_date) as completion_date 
                    FROM 
                        (select 
                            PP.doc_id,
                            PP.participant_id,
                            PP.study_id,
                            PP.remarks,
                            PP.document,
                            PP.doc_completion_date
                            FROM 
                            (select
                                Q.participant_id,
                                Q.study_id,
                                Q.doc_id,
                                Q.site_id,
                                case 
                                    when Q.uploaded_time is not null then 'uploaded'
                                    when Q.status = 'SignedByParticipant' then 'Self'
                                    when Q.status = 'SignedByPI' then 'Dual'   
                                    end as remarks,    
                                case
                                    when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                                    when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'
                                    end as document,  
                                case
                                    when Q.doc_completion_date LIKE ('%UTC%') then STR_TO_DATE(doc_completion_date, '%a %b %d %T UTC %Y')
                                    else Q.doc_completion_date
                                    end as doc_completion_date
                                    FROM
                                    (select *,
                                        max(created_time) as doc_completion_date
                                        FROM research.participant_study_document PSD
                                        WHERE PSD.participant_id is not null
                                          and PSD.study_id = ?
                                          ${params.participantsIds ? `and PSD.participant_id in (${this.getParticiaptnt(bindingParams)}) ` : ''}
                                          ${params.siteId ? `and PSD.site_id = '${params.siteId}' ` : ''}
                                        GROUP BY participant_id, created_time) Q
                                        WHERE Q.participant_id is not null) PP) PQ
                                    where PQ.document = 'econsent' ) P 
                    ON O.participant_id = P.participant_id and O.study_id = P.study_id
                INNER JOIN research_analytics.participant_site_country PSC 
                    ON O.participant_id = PSC.participant_id and O.study_id = PSC.study_id
                    WHERE completion_date <= CURDATE()
                    ${params.fromDate ? `AND completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''}
            ) ECS
        GROUP BY participant_id, country_id, site_id, activity_name, completion_date
        ) a
                UNION SELECT * FROM (
        SELECT
            study_id,
            participant_id,
            country_id,
            country_name,
            site_id,
            site_name,
            completion_date,
            activity_name,
            IFNULL(
                SUM(
                    case 
                        when remarks = 'uploaded' then 1 
                        when remarks = 'Dual' then 1 end)/COUNT(remarks)*100, 0)
                        as percent_completion
            FROM
            (SELECT 
                PP.doc_id,
                PP.doc_name as activity_name,
                PP.participant_id, 
                PP.study_id,
                PP.remarks,
                PP.document,
                DATE(PP.completion_date) as completion_date,
                PSC.country_id,
                PSC.country_name,
                PSC.site_id,
                PSC.site_name
                FROM 
                (select
                    Q.participant_id,
                    Q.study_id,
                    Q.doc_id, 
                    Q.doc_name,
                    Q.completion_date,
                    case 
                        when Q.uploaded_time is not null then 'uploaded'
                        when Q.status = 'SignedByParticipant' then 'Self'
                        when Q.status = 'SignedByPI' then 'Dual'
                        end as remarks, 
                    case
                        when LOWER(Q.s3object_key) LIKE ('%econsent%') then 'econsent'
                        when LOWER(Q.s3object_key) NOT LIKE ('%econsent%') then 'uploaded'   
                        end as document
                    from (
                    SELECT *, 
                        max(created_time) as completion_date 
                    FROM research.participant_study_document  P
                    WHERE P.participant_id is not null 
                        and P.study_id = ? 
                        ${params.participantsIds ? `and P.participant_id in (${participantsIds}) ` : ''}
                        ${params.siteId ? `and P.site_id = '${params.siteId}' ` : ''}
                    GROUP BY participant_id, created_time)  Q
                    WHERE Q.participant_id is not null
                ) PP 
            INNER JOIN research_analytics.participant_site_country PSC 
                ON PP.participant_id = PSC.participant_id and PP.study_id = PSC.study_id
            ) PQ
        WHERE completion_date <= CURDATE()
        ${params.fromDate ? `AND completion_date BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''}
        GROUP BY participant_id, country_id, site_id, activity_name, completion_date
        ) b 
                UNION SELECT * FROM (
        select
        study_id,
        participant_id,
        country_id,
        country_name,
        site_id,
        site_name, 
        completion_date,
        activity_name,
        IFNULL(COUNT(case when status = 'Complete' then 1 end)/COUNT(status)*100, 0) as percent_completion
        from
          (select
          P.participant_id,
          P.site_name,
          P.site_id,
          P.study_name,
          P.study_id,
          P.country_id,
          P.country_name,
          P.task_title as activity_name,
          coalesce(DATE(P.completion_date),
                  DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) as completion_date,
          CASE 
          WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status != 'Reschedule' THEN 'Complete'
          WHEN P.response_type = 'visits' AND P.ar_id IS NOT NULL AND P.visit_status = 'Reschedule' THEN P.visit_status 
          WHEN P.response_type = 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND end_day - start_day >= 1 THEN 'Missed'
          WHEN P.response_type = 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
          WHEN P.response_type = 'visits' AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          WHEN P.response_type != 'visits' AND P.ar_id IS NOT NULL THEN 'Complete' 
          WHEN P.response_type != 'visits' AND P.end_day = (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.end_day - P.start_day >= 1 THEN 'Missed'
          WHEN P.response_type != 'visits' AND P.end_day < (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) AND P.ar_id IS NULL THEN 'Missed' 
          WHEN P.response_type != 'visits'AND P.end_day >= (TIMESTAMPDIFF(day, P.participant_start_date, NOW())) THEN 'Scheduled' 
          END AS status
            from
              (select
              PSC.participant_id,
              PSC.participant_start_date,
              PSC.site_name,
              PSC.site_id,
              PSC.study_name,
              PSC.study_id,
              PSC.country_id,
              PSC.country_name,
              PTS.task_title,
              pts.created_date,
              pts.start_day,
              pts.end_day,
              case 
              when pts.task_type in ('survey', 'epro') then srt.id
              when pts.task_type = 'telehealth' then ppa.id
              when pts.task_type = 'activity' then ar.id
              end as ar_id,
              case 
              when pts.task_type in ('survey', 'epro') then 'survey'
              when pts.task_type = 'telehealth' then 'visits'
              when pts.task_type = 'activity' then 'activity'
              end as response_type,
              case 
              when pts.task_type = 'telehealth' then ppa.status
              else ''
              end as visit_status,
              case 
              when pts.task_type in ('survey', 'epro') then srt.completion_time_utc
              when pts.task_type = 'telehealth' then ppa.end_time
              when pts.task_type = 'activity' then ar.end_time
              end as completion_date
                from 
                (
                  select participant_id,
              participant_start_date, site_name, site_id, study_name, study_id, country_id, country_name from research_analytics.participant_site_country
                  where study_id = ?
                  ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                ) PSC
                INNER JOIN research_response.participant_task_schedule PTS ON PTS.study_id = ? and  PSC.participant_id = PTS.participant_id
                      and PSC.study_id = PTS.study_id
                LEFT JOIN research_response.activity_response ar on pts.participant_id = ar.participant_id
                      and pts.task_instance_id = ar.task_instance_id
                      and pts.study_version_id = ar.study_version_id
                LEFT JOIN research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                      and pts.task_instance_id = srt.task_instance_id
                      and pts.study_version_id = srt.study_version_id
                LEFT JOIN research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                      and pts.task_instance_id = ppa.task_instanceuuid
                      and pts.study_version_id = ppa.study_version_id
                      and (ppa.visit_id IS NOT NULL)
              ) P
              WHERE coalesce(DATE(P.completion_date),
              DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) <= CURDATE()
            ${params.fromDate ? `AND coalesce(DATE(P.completion_date), DATE_ADD(P.participant_start_date, INTERVAL P.end_day DAY)) 
                                    BETWEEN '${params.fromDate}' and '${params.toDate}' ` : ''}
          ) T1
            GROUP BY participant_id, country_id, site_id, activity_name, completion_date
        ) c)d)e
        GROUP BY participant_id, country_id, site_id
        ) A1
        GROUP BY participant_id, country_id, site_id
        `;
        console.log(`getCompletionDetailsByParticipant query SQL ${JSON.stringify(params)} \n${querySql} \n`);
        console.time(`getCompletionDetailsByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`getCompletionDetailsByParticipant query SQL ${JSON.stringify(params)} Ends in:`)
        console.log(`the length of getCompletionDetailsByParticipant is: ${JSON.stringify(data.length)}`)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getCompletionDetailsByParticipant]', er)
        throw er
      }
    }
}

module.exports = StudyComplianceModel;
