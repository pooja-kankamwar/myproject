const BaseModel = require("./baseModel");
const {constants: {DATABASE: {RESEARCH_ANALYTICS_DB}}} = require('../constants')
const {convertArrayToString} = require('../utils')
const {lodash: _} = require('../packages')

/**
 * Class representing a message model.
 * @class
 */
class EnrollmentModel extends BaseModel {
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

    async getRecruitmentData(params){
        const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
        try {
          const bindingParams = []
          let participantsIds = null;
          for (let index = 0; index < 9; index++) {
            bindingParams.push(params.studyId)
          }
          if (params.participantsIds) {
            participantsIds = convertArrayToString(params.participantsIds);
          }

          let querySql = `
          select
            tt1.study_id,
            tt1.participant_id as firstDataParticipant,
            tt1.first_data firstData,
            tt3.fpi_participant_id firstParticipant,
            tt3.fpi fpi,
            tt1.site_id,
            PSC.site_name,
            PSC.country_id,
            PSC.country_name,
            S.created_time active
          from
            (
            select
              *
            from
              (
              select
                ar.study_id,
                ar.participant_id,
                t2.first_data,
                t2.site_id
              from
                research_response.activity_response ar
              inner join (
                select
                  *
                from
                  (
                  select
                    t1.study_id,
                    min(t1.end_time) first_data,
                    PSC.site_id
                  from
                    research_response.activity_response t1
                  inner join (
                    select
                      participant_id,
                      site_id
                    from
                      research_analytics.participant_site_country
                    where
                      study_id = ?
                      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                      ${params.participantsIds ? `and participant_id IN (${participantsIds}) ` : ''} 
          ) PSC on
                    t1.participant_id = PSC.participant_id
                  group by
                    t1.study_id,
                    PSC.site_id ) t1
                where
                  first_data is not null) t2 on
                ar.study_id = t2.study_id
                and ar.end_time = t2.first_data ) t1
          union
            select
              *
            from
              (
              select
                srt.study_id,
                srt.participant_id,
                t2.first_data,
                t2.site_id
              from
                research_response.survey_response_tracker srt
              inner join (
                select
                  *
                from
                  (
                  select
                    t1.study_id,
                    min(t1.completion_time_utc) first_data,
                    PSC.site_id
                  from
                    research_response.survey_response_tracker t1
                  inner join (
                    select
                      participant_id,
                      site_id
                    from
                      research_analytics.participant_site_country
                    where
                      study_id = ?
                      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                      ${params.participantsIds ? `and participant_id IN (${participantsIds}) ` : ''} 
          ) PSC on
                    t1.participant_id = PSC.participant_id
                  group by
                    t1.study_id,
                    PSC.site_id ) t1
                where
                  first_data is not null) t2 on
                srt.study_id = t2.study_id
                and srt.completion_time_utc = t2.first_data ) t2
          union
            select
              *
            from
              (
              select
                ppi.study_id,
                ppi.participant_id,
                t2.first_data,
                t2.site_id
              from
                research_response.pi_participant_appointment ppi
              inner join (
                select
                  *
                from
                  (
                  select
                    t1.study_id,
                    min(t1.end_time) first_data,
                    PSC.site_id
                  from
                    research_response.pi_participant_appointment t1
                  inner join (
                    select
                      participant_id,
                      site_id
                    from
                      research_analytics.participant_site_country
                    where
                      study_id = ?
                      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                      ${params.participantsIds ? `and participant_id IN (${participantsIds}) ` : ''} 
          ) PSC on
                    t1.participant_id = PSC.participant_id
                  group by
                    t1.study_id,
                    PSC.site_id ) t1
                where
                  first_data is not null) t2 on
                ppi.study_id = t2.study_id
                and ppi.end_time = t2.first_data ) t3
            ) tt1
          inner join (
            select
              study_id,
              participant_id,
              min(first_data) first_data,
              site_id
            from
              (
              select
                *
              from
                (
                select
                  ar.study_id,
                  ar.participant_id,
                  t2.first_data,
                  t2.site_id
                from
                  research_response.activity_response ar
                inner join (
                  select
                    *
                  from
                    (
                    select
                      t1.study_id,
                      min(t1.end_time) first_data,
                      PSC.site_id
                    from
                      research_response.activity_response t1
                    inner join (
                      select
                        participant_id,
                        site_id
                      from
                        research_analytics.participant_site_country
                      where
                        study_id = ?
                        ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                        ${params.participantsIds ? `and participant_id IN (${participantsIds}) ` : ''} 
          ) PSC on
                      t1.participant_id = PSC.participant_id
                    group by
                      t1.study_id,
                      PSC.site_id ) t1
                  where
                    first_data is not null) t2 on
                  ar.study_id = t2.study_id
                  and ar.end_time = t2.first_data ) t1
            union
              select
                *
              from
                (
                select
                  srt.study_id,
                  srt.participant_id,
                  t2.first_data,
                  t2.site_id
                from
                  research_response.survey_response_tracker srt
                inner join (
                  select
                    *
                  from
                    (
                    select
                      t1.study_id,
                      min(t1.completion_time_utc) first_data,
                      PSC.site_id
                    from
                      research_response.survey_response_tracker t1
                    inner join (
                      select
                        participant_id,
                        site_id
                      from
                        research_analytics.participant_site_country
                      where
                        study_id = ?
                        ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                        ${params.participantsIds ? `and participant_id IN (${participantsIds}) ` : ''} 
                        ) PSC on
                      t1.participant_id = PSC.participant_id
                    group by
                      t1.study_id,
                      PSC.site_id ) t1
                  where
                    first_data is not null) t2 on
                  srt.study_id = t2.study_id
                  and srt.completion_time_utc = t2.first_data ) t2
            union
              select
                *
              from
                (
                select
                  ppi.study_id,
                  ppi.participant_id,
                  t2.first_data,
                  t2.site_id
                from
                  research_response.pi_participant_appointment ppi
                inner join (
                  select
                    *
                  from
                    (
                    select
                      t1.study_id,
                      min(t1.end_time) first_data,
                      PSC.site_id
                    from
                      research_response.pi_participant_appointment t1
                    inner join (
                      select
                        participant_id,
                        site_id
                      from
                        research_analytics.participant_site_country
                      where
                        study_id = ?
                        ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                        ${params.participantsIds ? `and participant_id IN (${participantsIds}) ` : ''} 
          ) PSC on
                      t1.participant_id = PSC.participant_id
                    group by
                      t1.study_id,
                      PSC.site_id ) t1
                  where
                    first_data is not null) t2 on
                  ppi.study_id = t2.study_id
                  and ppi.end_time = t2.first_data ) t3
              ) t1
            group by
              study_id,
              site_id) tt2 on
            tt1.site_id = tt2.site_id
            and tt1.first_data = tt2.first_data
          left join research_analytics.participant_site_country PSC on
            tt1.participant_id = PSC.participant_id
          left join research.Site S on tt1.site_id = S.id
          left join (
          select 
          P.participant_id as fpi_participant_id,
          P.study_id,
          t2.site_id,
          t2.fpi
          from
          (select psh.participant_id,psh.study_id, psh.modified_time, PSC.site_id
            from research.participant_status_history psh
            inner join  research_analytics.participant_site_country PSC on PSC.participant_id = psh.participant_id
            where psh.study_id = ?
              and psh.new_status = 'ACTIVE'
              
              group by psh.participant_id) P
          inner JOIN
          (select   min(t1.modified_time) fpi, t1.site_id
          from
          (select psh.participant_id, psh.modified_time, PSC.site_id
          from research.participant_status_history psh
          inner join  research_analytics.participant_site_country PSC on PSC.participant_id = psh.participant_id
          where psh.study_id = ?
            and psh.new_status = 'ACTIVE'
            group by psh.participant_id) t1
            group by site_id) t2 on P.site_id = t2.site_id and P.modified_time = t2.fpi
            ) tt3 on tt1.site_id = tt3.site_id
          ${params.fromDate ? `where COALESCE(tt1.first_data,S.created_time) between '${params.fromDate}' and DATE_ADD('${params.toDate}', INTERVAL 1 DAY) `: ''} 
          and S.status != 'PENDING'
          `
          console.log(`getRecruitmentData query SQL ${JSON.stringify(params)} \n${querySql}`);
          const [data] = await dbConnectionPool.query(querySql, bindingParams)
          dbConnectionPool.end();
          return data;
        } catch (error) {
          dbConnectionPool.end();
          console.log('Error in function getRecruitmentData:', error);
          throw error;
        }
    }

    async getConsentData(studyId, siteId){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        bindingParams.push(studyId)

        let querySql = `
        select study_id,
        IFNULL(country_name, 'UnknownCountry') as country_name,
        IFNULL(site_name, 'UnknownSite') as site_name,
        IFNULL(n_document_econsent, 0) as econsent,
        IFNULL(n_document_uploaded, 0) as uploaded,
        IFNULL(n_self_econsent, 0) as self,
        IFNULL(n_dual_econsent, 0) as 'dual',
        IFNULL(n_document_econsent + n_document_uploaded, 0) as total,
        IFNULL(n_quiz_complete_participant, 0) as quizQompletion,
        IFNULL(n_attempt_participant, 0) as quizAttempts,
        IFNULL(n_attempt_participant - n_quiz_complete_participant, 0) as quizIncomplete,
        updated_at as updated_date
        from enrollment
        where study_id = ?
        `

        if (siteId) {
          bindingParams.push(siteId)
          querySql += ' AND site_id = ? '
        }
        console.log(`getConsentData query SQL ${studyId}-${siteId} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function getConsentData:', error);
        throw error;
      }
    }

    async getQuizCompletionData(params){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        bindingParams.push(params.studyId)

        let querySql = `
        SELECT study_id, IFNULL(country_name, 'UnknownCountry') as country_name , IFNULL(site_name, 'UnknownSite') as site_name, 
        CAST(COUNT(CASE WHEN econsent_quiz_completion = 'complete' THEN 1 else null END) as SIGNED) as quizQompletion,
        CAST(SUM(econsent_quiz_attempts) as SIGNED) as quizAttempts,
        CAST((SUM(econsent_quiz_attempts) - COUNT(CASE WHEN econsent_quiz_completion = 'complete' THEN 1 else null END)) as SIGNED) as quizIncomplete
        from research_analytics.enrollment_participant
        where study_id = ? 
        `

        if (params.siteId) {
          bindingParams.push(params.siteId)
          querySql += ' AND site_id = ? '
        }
        if (params.fromDate) {
          bindingParams.push(params.fromDate)
          bindingParams.push(params.toDate)
          querySql += 'AND date(econsent_quiz_completion_date) between ? and ?'
        }
        querySql += 'group by study_id, site_id, country_id'
        console.log(`getQuizCompletionData query SQL ${JSON.stringify(params)} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function getQuizCompletionData:', error);
        throw error;
      }
    }

    async getParticipantStatusData(params){
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

        const querySql = `
            SELECT
            P.study_id as studyId,
            PSC.country_id as countryId,
            PSC.country_name as countryName,
            PSC.site_id,
            PSC.site_name as siteName,
            S.type as siteType,
            IFNULL(S.address, '') as siteAddress,
            IFNULL(S.city, '') as siteCity,
            IFNULL(S.zipcode, '') as postalCode,
            IFNULL(S.state, '') as siteState,
            DATE(S.created_time) as createdDate,
            CASE WHEN UPPER(P.status) = 'NOTINVITED' then 'IMPORTED'
            ELSE P.status END as status,
            COUNT(P.id) as n_participant            FROM (SELECT * FROM research.participant where study_id = ? ) P
            LEFT JOIN (SELECT * FROM research_analytics.participant_site_country where study_id = ? ) PSC ON P.id = PSC.participant_id and P.study_id = PSC.study_id
            LEFT JOIN research.site S ON P.site_id = S.id
            ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
            ${params.siteId ? `PSC.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
            ${params.fromDate ? `(DATE(COALESCE(P.invitation_date,P.registration_date)) BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
            ${params.participantsIds ? `P.id IN (${participantsIds}) ` : ''} 
            
            GROUP BY P.study_id, P.status, PSC.country_id, PSC.site_id
        `
        console.log(`getParticipantStatusData query SQL ${JSON.stringify(params)} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function getParticipantStatusData:', error);
        throw error;
      }
    }

    async getScreenFailuresReasons(params){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = [];
        let participantIds = null;
        bindingParams.push(params.studyId);
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }

        const querySql = `
        select
        PSH.participant_id,
        PSC.study_id,
        PSC.study_name, 
        PSC.site_id,
        PSC.site_name as siteName,
        PSC.country_id,
        PSC.country_name as countryName,
        PSH.new_status,
        case 
        when PSH.status_change_reason is null then 'Screen Failure'
        ELSE PSH.status_change_reason
        END as statusChangeReason,
        COUNT(P.id) as 'n_participant'

        from (select * from research.participant
            where study_id = ?) P

        LEFT JOIN research_analytics.participant_site_country PSC on P.study_id = PSC.study_id
                and P.site_id = PSC.site_id
                and P.id = PSC.participant_id
        LEFT JOIN research.participant_status_history PSH ON PSH.study_id = PSC.study_id
        and PSH.participant_id = P.id

        WHERE PSH.new_status = 'SCREENFAILED'
        ${params.participantsIds ? `and PSH.participant_id IN (${participantsIds}) ` : ''} 
        ${params.siteId ? `and PSC.site_id = '${params.siteId}' ` : ''}
        ${params.fromDate ? `and PSH.modified_time between '${params.fromDate}' and '${params.toDate}' ` : ''} 
        GROUP BY P.study_id, P.site_id, PSH.status_change_reason
        ORDER BY count(id) DESC
        `;
        console.log(`getScreenFailuresReasons query SQL ${JSON.stringify(params)} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function getScreenFailuresReasons:', error);
        throw error;
      }
    }

    async getWithdrawalReasons (studyId) {
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        bindingParams.push(studyId)

        const querySql = `
          SELECT
            e.study_id AS "study_id",
            IFNULL(e.country_name, 'UnknownCountry') AS "country_name",
            IFNULL(e.site_name, 'UnknownSite') AS "site_name",
            IFNULL(e.n_participant_stats_withdrawal, 0) AS "withdrawal",
            IFNULL(e.n_withdrawal_reasons_col, '-') AS "withdrawal_reasons",
            IFNULL(e.n_withdrawal_reasons_val, '-') AS "total_withdrawal_reasons",
            e.updated_at AS "updated_date"
          FROM
            enrollment e
          WHERE
            e.study_id = ?
          ORDER BY
            e.country_name ASC, e.site_name ASC;
        `
        console.log(`getWithdrawalReasons query SQL ${studyId} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getWithdrawalReasons]', er)
        throw er
      }
    }

    async getAllSitesFromActivityTable (studyId) {
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        bindingParams.push(studyId)
        const querySql = `
          SELECT
            DISTINCT(IFNULL(site_name, 'UnknownSite')) as siteName,
            IFNULL(country_name, 'UnknownCountry') as countryName
          FROM
            activity_history
          WHERE
            study_id = ?
          ORDER BY
            site_name ASC;
        `
        console.log(`getAllSitesFromActivityTable query SQL ${studyId} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in model function getAllSitesFromActivityTable]', er)
        throw er
      }
    }

    async getAllStudyActivities (params) {
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        let bindingParams = [];
        let participantIds = null;
        if(_.isNaN(params.activityPage)) params.activityPage = 1
        if(_.isNaN(params.activityLimit)) params.activityLimit = 30
        let activityOffset = (params.activityPage-1) * params.activityLimit
        let querySql = null
        let data = null

        bindingParams = []
        querySql = `
        select
          TS.task_id,
          TS.task_instance_id,
          TS.task_title,
          TS.start_day,
          TS.sequence,
          MC.max_counter,
          case 
            when TS.start_day = MC.min_start_day then 1
            when TS.start_day = MC.max_start_day then MC.max_counter
            else
              ROUND(( (TS.start_day - MC.min_start_day) / (MC.max_start_day - MC.min_start_day) * (MC.max_counter - 1) ) + 1)
            end as current_counter
        from
          (
          select
            distinct task_id,
            task_instance_id,
            task_title,
            start_day,
            sequence
          from
            research.task_schedules TS
          where
            study_id = ${this.setBindingParams(bindingParams, params.studyId)}
            order by
            start_day,
            sequence,
            task_title
            limit ${this.setBindingParams(bindingParams, activityOffset)},${this.setBindingParams(bindingParams, params.activityLimit)}) TS
        left join (
          select
            task_id,
            count(distinct(task_instance_id)) as max_counter,
            min(start_day) as min_start_day,
            max(start_day) as max_start_day
          from
            (
            select
              distinct task_id,
              task_instance_id,
              task_title,
              start_day,
              sequence
            from
              research.task_schedules
            where
              study_id = ${this.setBindingParams(bindingParams, params.studyId)}) t1
          group by
            task_id) MC on
          TS.task_id = MC.task_id
        `
        console.log(`getAllStudyActivities query SQL ${JSON.stringify(params)} \n${querySql}`);
        console.log(`getAllStudyActivities binding params ${params.studyId} \n${JSON.stringify(bindingParams)}`);
        console.time(`getAllStudyActivities query SQL time ${params.studyId} Ends in:`)
        const result = await dbConnectionPool.query(querySql, bindingParams)
        data = result[0]
        console.timeEnd(`getAllStudyActivities query SQL time ${params.studyId} Ends in:`)
        console.log(`length getAllStudyActivities query SQL time:`, data)
        

        dbConnectionPool.end()
        return data
      } catch (er) {
        dbConnectionPool.end()
        console.log('[Error in function getAllStudyActivities]', er)
        throw er
      }
    }

    async getParticipantProgress (params) {
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        let bindingParams = [];
        let participantIds = null;
        if(_.isNaN(params.page)) params.page = 1
        if(_.isNaN(params.limit)) params.limit = 10
        let offset = (params.page-1) * params.limit
        let querySql = `
        select 
          pts.task_id,pts.task_instance_id,pts.participant_id as participantId, pts.country_name as countryName, pts.site_name as siteName,
          CASE
                WHEN  ppa.id IS NOT NULL AND ppa.status in ('Missed', 'Cancelled', 'SiteCancelled') THEN 'NotComplete'
                WHEN  ppa.id IS NOT NULL AND ppa.status = 'Complete' THEN 'Completed'
                WHEN  (srt.id IS NOT NULL OR ar.id IS NOT NULL) THEN 'Completed'
                WHEN  pts.start_day <= (TIMESTAMPDIFF(day,pts.participant_start_date, NOW())) AND   pts.end_day > (TIMESTAMPDIFF(day,pts.participant_start_date, NOW())) THEN 'Ongoing'
                WHEN  pts.end_day = (TIMESTAMPDIFF(day,pts.participant_start_date, NOW())) AND pts.end_day - pts.start_day >= 1  THEN 'NotComplete' 
                WHEN  pts.end_day < (TIMESTAMPDIFF(day,pts.participant_start_date, NOW())) AND (srt.id IS NULL OR ar.id IS NULL OR ppa.id IS NULL) THEN 'NotComplete'   
                WHEN  pts.end_day >= (TIMESTAMPDIFF(day,pts.participant_start_date, NOW())) THEN 'Upcoming'
                END AS status,pts.participant_end_day
          from 
          (select 
          DISTINCT pts1.task_id, 
          pts1.task_instance_id, 
          pts1.start_day, 
          pts1.end_day, 
          pts1.participant_id, 
          pts1.study_version_id, 
          pts1.participant_start_date, 
          pts1.country_name, 
          pts1.site_name, 
          case when t2.discountinued_date is not null then TIMESTAMPDIFF(
          DAY, pts1.participant_start_date, 
          t2.discountinued_date) 
          when t2.discountinued_date is null then (select max(end_day) 
          from 
          research_response.participant_task_schedule 
          where 
          study_id = ${this.setBindingParams(bindingParams, params.studyId) }
          ) end as participant_end_day, 
          t2.discountinued_date  
          from
          (select
          t1.task_id,t1.task_instance_id,t1.start_day,t1.end_day,t1.participant_id, t1.study_version_id,
          PSC.participant_start_date, PSC.country_name, PSC.site_name
          from
          (
          select task_id,task_instance_id,start_day,end_day,participant_id,study_version_id
          from
          research_response.participant_task_schedule
          where 
          study_id = ${this.setBindingParams(bindingParams, params.studyId)}
          and participant_id IN (${this.setBindingParams(bindingParams, params.listParticipantIds)})   
          and task_instance_id in (${this.setBindingParams(bindingParams, params.listTaskInstanceIds)})   
            ) t1
          LEFT JOIN (
          select * from research_analytics.participant_site_country
          where study_id = ${this.setBindingParams(bindingParams, params.studyId)}
          ${params.participantIds ? `and participant_id IN (${this.setBindingParams(bindingParams, params.participantIds)}) ` : ''} 
          ${params.siteId ? `and site_id = ${this.setBindingParams(bindingParams, params.siteId)} ` : ''}
          ${params.fromDate ? `and enrollment_date between ${this.setBindingParams(bindingParams, params.fromDate)} and ${this.setBindingParams(bindingParams, params.toDate)} ` : ''} 
          and participant_start_date is not null
          order by enrollment_date
          limit ${this.setBindingParams(bindingParams, offset)},${this.setBindingParams(bindingParams, params.limit)}
          ) PSC on t1.participant_id = PSC.participant_id
          ) pts1 
          left join (
          SELECT 
          participant_id, 
          modified_time as discountinued_date 
          FROM 
          research.participant_status_history 
          where 
          study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
          and new_status in (
          'DISCONTINUED', 'DISQUALIFIED', 'WITHDRAWSTUDY'
          )
          ) t2 on pts1.participant_id = t2.participant_id
          ) pts
          left join
                research_response.survey_response_tracker srt on  pts.participant_id = srt.participant_id and pts.task_instance_id = srt.task_instance_id  and pts.study_version_id = srt.study_version_id 
          left join 
                research_response.activity_response ar on  pts.participant_id = ar.participant_id and pts.task_instance_id = ar.task_instance_id and pts.study_version_id = ar.study_version_id 
          left join research_response.pi_participant_appointment ppa on  pts.participant_id = ppa.participant_id and pts.task_instance_id = ppa.task_instanceuuid and pts.study_version_id = ppa.study_version_id 
                and  (ppa.visit_id IS NOT NULL)
          where 
          pts.end_day <=pts.participant_end_day
        `
        console.log(`getParticipantProgress query SQL for id ${params.studyId} : ${JSON.stringify(params)} \n${querySql}`);
        console.log(`getParticipantProgress binding params ${params.studyId} \n${JSON.stringify(bindingParams)}`);
        console.time(`getParticipantProgress query SQL time ${params.studyId} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        console.timeEnd(`getParticipantProgress query SQL time ${params.studyId} Ends in:`)
        dbConnectionPool.end();
        console.log('Data => ', data)
        return data
      } catch (err) {
        dbConnectionPool.end()
        console.log('[Error in function getParticipantProgress]', err)
        throw err
      }
    }
    
    async getParticipants(params){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try{
        const bindingParams = []
        if(_.isNaN(params.page)) params.page = 1
        if(_.isNaN(params.limit)) params.limit = 10
        let offset = (params.page-1) * params.limit
        let querySql = `
        select id, countryName, siteName,
        case when discountinued_date is not null then TIMESTAMPDIFF(DAY,t1.enrollment_date,t2.discountinued_date)
             when  t2.discountinued_date is null then (select max(end_day) from research_response.participant_task_schedule where study_id=${this.setBindingParams(bindingParams, params.studyId)})
             end as participant_end_day
        from
        (
          select psc.participant_id as id, psc.country_name as countryName, psc.site_name as siteName, psc.enrollment_date as enrollment_date
          from research_analytics.participant_site_country psc
          left join(select distinct (participant_id) from research_response.participant_task_schedule
           )pts on psc.participant_id = pts.participant_id
          where study_id = ${this.setBindingParams(bindingParams, params.studyId)}
          and pts.participant_id is not null
          ${params.participantIds ? `and participant_id IN (${this.setBindingParams(bindingParams, params.participantIds)}) ` : ''} 
          ${params.siteId ? `and site_id = ${this.setBindingParams(bindingParams, params.siteId)} ` : ''}
          ${params.fromDate ? `and enrollment_date between ${this.setBindingParams(bindingParams, params.fromDate)} and ${this.setBindingParams(bindingParams, params.toDate)} ` : ''}
		      and participant_start_date is not null
          order by enrollment_date
          limit ${this.setBindingParams(bindingParams, offset)},${this.setBindingParams(bindingParams, params.limit)}
          )t1
          left join (
             SELECT 
             participant_id, 
             modified_time as discountinued_date 
             FROM 
             research.participant_status_history 
             where 
             study_id = ${this.setBindingParams(bindingParams, params.studyId)} 
             and new_status in (
             'DISCONTINUED', 'DISQUALIFIED', 'WITHDRAWSTUDY'
             )
             ) t2 on t1.id = t2.participant_id
          `

        console.log(`getParticipants query SQL for id ${params.studyId} : ${JSON.stringify(params)} \n${querySql}`);
        console.log(`getParticipants binding params ${params.studyId} \n${JSON.stringify(bindingParams)}`);
        console.time(`getParticipants query SQL time ${params.studyId} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        console.timeEnd(`getParticipants query SQL time ${params.studyId} Ends in:`)
        dbConnectionPool.end();
        return data
      } catch (err) {
        dbConnectionPool.end()
        console.log('[Error in function enrollment - getParticipants]', err)
        throw err
      }
    }

    async getOverallEnrollment (params) {
      const dbConnection = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        const bindingParams1 = []
        let query = `
          SELECT QD.site_id, QD.study_id ,SC.site_name, IFNULL(P.n_participant, 0) AS n_participant, QD.month, QD.year,
          SC.country_name
          from
          (  
          select d1.study_id,d1.site_id, d1.month, d1.year
          from
          (select distinct P.study_id,P. P.site_id, D.month, D.year 
          from
            (SELECT distinct
                            PSH.site_id,
                            PSH.study_id,
                            EXTRACT(MONTH from COALESCE(invitation_date,registration_date)) AS "month",
                            EXTRACT(year  from COALESCE(invitation_date,registration_date)) AS year
                            FROM research.participant PSH
                            WHERE
                            COALESCE(invitation_date,registration_date) is not null and 
                            PSH.study_id = ${this.setBindingParams(bindingParams, params.studyId)}
                            ${params.participantIds ? `and PSH.id IN (${this.setBindingParams(bindingParams, params.participantIds)}) ` : ''} 
                            ${params.siteIds ? `and PSH.site_id IN (${this.setBindingParams(bindingParams, params.siteIds)}) ` : ''}
                            ${params.countryIds ? `and PSH.country_id IN (${this.setBindingParams(bindingParams, params.countryIds)}) ` : ''}
                            ) P , (
            select DISTINCT EXTRACT(MONTH from gen_date) AS "month",
                            EXTRACT(year  from gen_date) AS year from 
            (select adddate('2000-01-01',t4*10000 + t3*1000 + t2*100 + t1*10 + t0) gen_date from
            (select 0 t0 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
            (select 0 t1 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
            (select 0 t2 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
            (select 0 t3 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
            (select 0 t4 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
            where gen_date between '2000-01-01' and (select MAX(COALESCE(invitation_date,registration_date)) from research.participant)
            ) D) d1
            LEFT JOIN (
            select PSH.study_id,
            PSH.site_id, min(COALESCE(PSH.invitation_date,PSH.registration_date)) as min_time
            from research.participant PSH
            where PSH.study_id is not null
            group by PSH.study_id, PSH.site_id
            ) d2 ON d1.site_id = d2.site_id and d1.study_id = d2.study_id
            WHERE (d1.year >= EXTRACT(YEAR from d2.min_time) and d1.month >= EXTRACT(MONTH from d2.min_time))
            OR    (d1.year > EXTRACT(YEAR from d2.min_time) and d1.month < EXTRACT(MONTH from d2.min_time))
                            ) QD
            LEFT JOIN 
            (SELECT
                            site_id,
                            study_id,
                            count(participant_id) as n_participant,
                            month,
                            year
                        FROM
                        (
                        (
                            SELECT
                            PSH.id,
                            PSH.id as participant_id,
                            PSH.site_id,
                            PSH.study_id,
                            COALESCE(PSH.invitation_date,PSH.registration_date),
                            EXTRACT(MONTH from COALESCE(invitation_date,registration_date)) AS "month",
                            EXTRACT(year  from COALESCE(invitation_date,registration_date)) AS year
                            FROM research.participant PSH
                            WHERE
                            COALESCE(invitation_date,registration_date) is not null and
                            PSH.study_id = ${this.setBindingParams(bindingParams, params.studyId)}
                            ${params.participantIds ? `and PSH.id IN (${this.setBindingParams(bindingParams, params.participantIds)}) ` : ''} 
                            ${params.siteIds ? `and PSH.site_id IN (${this.setBindingParams(bindingParams, params.siteIds)}) ` : ''}
                            ${params.countryIds ? `and PSH.country_id IN (${this.setBindingParams(bindingParams, params.countryIds)}) ` : ''}
                            )
                        ) P
                        GROUP BY study_id,site_id, month, year
                        ORDER BY year ASC, month ASC) P
            ON QD.year = P.year and QD.month = P.month and QD.site_id = P.site_id and QD.study_id = P.study_id 
            LEFT JOIN (
            select S.id as site_id, S.name as site_name, C.country_name
            from
            research.site S
            LEFT JOIN research.site_country SC ON S.id = SC.site_id
            LEFT JOIN research.country C on SC.country_id = C.country_id
            group by S.id
            ) SC on QD.site_id = SC.site_id
          ORDER BY QD.YEAR ASC, QD.MONTH ASC`
        console.log(`getOverallEnrollment query SQL ${JSON.stringify(params)} \n${query}`);
        const [overallSitesData] = await dbConnection.query(query, bindingParams);
        let query1 = `
            select EXTRACT(MONTH from SA.created_time) AS 'month',
            EXTRACT(year  from SA.created_time) AS year, 
            S.name as site_name, SA.created_time as status_update_time,
            JSON_EXTRACT(SA.new_value, '$.status') as site_status
            from research.site_audit_trail SA
            LEFT JOIN research.study_site SS ON SA.site_id = SS.site_id
            LEFT JOIN research.site S ON SA.site_id = S.id
            where SA.event_type = 'SITE_STATUS_CHANGED'  
            and SS.study_id = ${this.setBindingParams(bindingParams1, params.studyId)}
            ${params.siteIds ? `and SA.site_id IN (${this.setBindingParams(bindingParams1, params.siteIds)}) ` : ''}
            GROUP BY SA.site_id, month, year, site_status
            ORDER BY SA.site_id, year ASC, month ASC
          `
        console.log(`getOverallEnrollment inactive site query SQL ${JSON.stringify(params)} \n${query1}`);
        const [siteStatusChangeData] = await dbConnection.query(query1, bindingParams1);
        dbConnection.end();
        const data = {overallSitesData,
                      siteStatusChangeData
                     }
        // console.log('Data => ', data)
        return data
      } catch (err) {
        dbConnection.end()
        console.log('[Error in function getOverallEnrollment]', err)
        throw err
      }
    }

    async  getNoncomplianceParticipantRetentionRate (params) {
      const dbConnection = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);

      try {
        let participantsIds = null;
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select
          country_name,site_name,participant_id,
          COUNT(case when status = 'Missed' then 1 end ) as missed,
          COUNT(case when status = 'Cancelled' then 1 end ) as Cancelle,
          COUNT(case when status = 'Complete' then 1 end ) as Complete,
          ((COUNT(case when status = 'Missed' then 1 end )+COUNT(case when status = 'Cancelled' then 1 end ))
          /(COUNT(case when status = 'Missed' then 1 end )+COUNT(case when status = 'Cancelled' then 1 end )+COUNT(case when status = 'Complete' then 1 end ) ))*100  as Non_Compliance,
          ((DATEDIFF(disenrollment_date, active_date) ) /(DATEDIFF(first_patient_last_date, first_patient_enroll_date) ) ) * 100 as participantRetentionRate
          from (select
                  P.participant_id,
                  P.site_name,
                  P.site_id,
                  P.country_name,
                  P.active_date,
                  P.end_day,
                  P.start_day,
                  P.task_type,
                  P.visit_status,
                  P.disenrollment_date,
          CASE
					WHEN P.task_type = 'telehealth' AND P.ar_id IS NOT NULL AND P.visit_status not in ( 'Reschedule','Cancelled') THEN 'Complete'
          WHEN P.task_type = 'telehealth' AND P.ar_id IS not  NULL AND P.visit_status = 'Cancelled' THEN 'Cancelled'
					WHEN P.task_type = 'telehealth' AND P.end_day <= (TIMESTAMPDIFF( day, P.participant_start_date, COALESCE(DATE (P.disenrollment_date),CURDATE()))) AND P.ar_id IS NULL THEN 'Missed'
          WHEN P.task_type = 'telehealth' AND P.end_day > (TIMESTAMPDIFF(day, P.participant_start_date, COALESCE(DATE (P.disenrollment_date),CURDATE()))) THEN 'Scheduled'
          WHEN P.task_type != 'telehealth' AND P.ar_id IS NOT NULL THEN 'Complete'
          WHEN P.task_type != 'telehealth' AND P.end_day <= (TIMESTAMPDIFF(day, P.participant_start_date, COALESCE(DATE (P.disenrollment_date),CURDATE()))) AND P.ar_id IS NULL THEN 'Missed'
          WHEN P.task_type != 'telehealth' AND P.end_day > (TIMESTAMPDIFF(day, P.participant_start_date, COALESCE(DATE (P.disenrollment_date),CURDATE()))) THEN 'Scheduled'
          END AS status,
          P.first_patient_last_date, P.first_patient_enroll_date
          from
              (select
                      PSC.participant_id,
                      PSC.site_name,
                      PSC.site_id,
                      PSC.country_name,
                      pts.start_day,
                      pts.end_day,
                      psc.active_date,
                      PTS.task_type,
                      ppa.status as visit_status,
                      pts.task_instance_id,
                      psc.disenrollment_date,
                      PSC.participant_start_date,
                      smd.first_patient_last_date, smd.first_patient_enroll_date,
           case 
           when pts.task_type in ('survey', 'epro') then srt.id
					 when pts.task_type = 'telehealth' then ppa.id
					 when pts.task_type = 'activity' then ar.id end as ar_id
           from
            research_analytics.participant_site_country PSC
            inner join research_response.participant_task_schedule PTS ON PSC.participant_id = PTS.participant_id
            and PSC.study_id = PTS.study_id
                                      
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
            LEFT JOIN research.study_meta_data smd on smd.id=psc.study_id
            where psc.study_id =  '${params.studyId}'
            and  pts.study_id =  '${params.studyId}' and
                   
             psc.active_date is not null AND psc.disenrollment_date is not null
             ${params.siteId ? ` and PSC.site_id = '${params.siteId}'`:''}
             ${params.fromDate ? `and (PSC.disenrollment_date BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''}
             ${params.participantsIds ? ` and PSC.participant_id in (${participantsIds})`:''}  
            ) P
              WHERE
              DATE_ADD(
               P.active_date, INTERVAL P.end_day - 1 DAY
               ) <= coalesce(DATE(disenrollment_date), now())
               )f5 group by participant_id
        `
        console.log(`getNoncomplianceParticipantRetentionRate query SQL ${JSON.stringify(params)} \n${querySql}`);
        const [data] = await dbConnection.query(querySql)
        dbConnection.end();
        return data;
      }catch (error) {
        dbConnection.end();
        console.log('Error in function getNoncomplianceParticipantRetentionRate:', error);
        throw error;
      }
    }
    
    async getTopReasonsForScreenFailures (params) {
      const dbConnection = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      let participantsIds = null;
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
      try {
        let querySql = `
               SELECT
                  T1.site_id as siteId,
                  T1.site_name as siteName,
                  T1.country_name as countryName,
                  T1.reason_text as statusChangeReason,
                  IFNULL(T2.count,0) as totalCount
              from
              ( select * from
                      (select distinct(PSCR.site_id) site_id,country_name,site_name
                      FROM research.participant_status_change_reasons PSCR
                      LEFT JOIN research_analytics.participant_site_country PSC 
                      on PSC.participant_id = PSCR.participant_id  
                    where PSC.study_id = '${params.studyId}'
                    and PSCR.new_status='SCREENFAILED'
                    ${params.siteId ? ` and PSC.site_id = '${params.siteId}'` : ''}
                    ${params.fromDate ? `and (DATE(PSCR.created_time) BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''}
                    ${params.participantsIds ? ` and PSCR.participant_id in (${participantsIds})` : ''}
                  )ST1,
                      (select distinct(reason_text) as reason_text
                    from research.participant_status_change_reasons
                          where new_status='SCREENFAILED'
                          and reason_text is not null 
                          and reason_text != ''
                  )ST2
                      
              )T1
              left join 
              (select
                  site_id as siteId,
                  reason_text as reason_text,
                  count(id) as count
                  FROM research.participant_status_change_reasons PSCR
                      where PSCR.new_status='SCREENFAILED'  
                      and PSCR.study_id= '${params.studyId}'
                      ${params.siteId ? ` and PSCR.site_id = '${params.siteId}'` : ''}
                      ${params.fromDate ? `and (DATE(PSCR.created_time) BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''}
                      ${params.participantsIds ? ` and PSCR.participant_id in (${participantsIds})` : ''}
                      group by site_id, reason_text
               ) T2 on T1.site_id=T2.siteId and T1.reason_text=T2.reason_text
        `
        console.log(`getTopReasonsForScreenFailures query SQL ${JSON.stringify(params)} \n${querySql}`);
        const [data] = await dbConnection.query(querySql)
        dbConnection.end();
        return data;
      }catch (error) {
        dbConnection.end();
        console.log('Error in function getTopReasonsForScreenFailures:', error);
        throw error;
      }
    }

}

module.exports = EnrollmentModel;
