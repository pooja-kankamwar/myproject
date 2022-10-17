const BaseModel = require("./baseModel");
const {constants: {DATABASE: {RESEARCH_ANALYTICS_DB}}} = require('../constants')
const {convertArrayToString} = require('../utils')

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

    async getQueriesData(params){
        const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
        try {
          const bindingParams = []
          let participantsIds = null;
          for (let index = 0; index < 24; index++) {
            bindingParams.push(params.studyId)
          }
          if (params.participantsIds) {
            participantsIds = convertArrayToString(params.participantsIds);
          }
          let querySql = `
          select 
          t1.country_name as countryName,t1.site_name as siteName, t1.activity_name as activityName,t1.Bucket_type as bucketType, IFNULL(t2.n_queries_open,0) n_queries_open,
          IFNULL(t2.n_responded_not_closed,0) n_responded_not_closed, IFNULL(t2.n_closed_query,0) n_closed_query
          from
          (
          select 
          *
          from (select distinct(site_name) site_name,country_name from (
          select
          t1.query_id,t2.participant_id, t1.status old_status, t1.response_id, t1.study_id,t1.created_at ,
          t2.form_id,  t2.activity_name, QRE.query_response_id,
          t2.country_id,t2.country_name,t2.site_id,t2.site_name,t2.study_name, 
          CASE
              WHEN QRE.query_response_id is null AND t1.status = 'OPEN' THEN 'OPEN'
              WHEN QRE.query_response_id is not null AND t1.status = 'OPEN' THEN 'RESPONDED NOT CLOSED'
              WHEN t1.status = 'CLOSE' THEN 'CLOSE'
              END AS new_status,
          CASE
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) < 2 THEN '0-1 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 1 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 5 THEN '>1-4 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 4 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 11 THEN '>4-10 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 10 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 21 THEN '>10-20 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 20 THEN '>20 days'
              END AS Bucket_type
          from
          (
          select id as query_id, status, response_id, study_id,created_at
          from research_response.edc_query
          where study_id = ?
          ) t1
          LEFT JOIN
          (select 
          QR.response_id, QR.form_id, QR.study_id, QF.activity_name, QR.participant_id,
          PSC.country_id,PSC.country_name,PSC.site_id,PSC.site_name,PSC.study_name
          from
          (select id as response_id, form_id, study_id, participant_id,submitted_at
          from 
          research_response.edc_response
          where study_id = ?) QR
          LEFT JOIN (select 
          id,study_id,created_at, name activity_name
          from
          research_response.edc_form
          where study_id = ?) QF ON QR.form_id = QF.id
          LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id and
                                                                      QR.study_id = PSC.study_id
          ) t2 ON t1.response_id = t2.response_id
          LEFT JOIN (
          select 
          EQ.id query_response_id
          from 
          research_response.edc_query EQ
          LEFT JOIN research_response.edc_query_comment EQC ON EQ.id = EQC.edc_query_id 
          where  
          EQC.created_by != EQ.created_by
          and EQ.study_id = ?
          group by query_response_id
          ) QRE ON t1.query_id = QRE.query_response_id
          ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
          ${params.siteId ? `t2.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
          ${params.fromDate ? `(t1.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
          ${params.participantsIds ? `t2.participant_id in (${participantsIds}) ` : ''} 
          ) 
          t1) t3, (select distinct(activity_name) activity_name from (
          select
          t1.query_id,t2.participant_id, t1.status old_status, t1.response_id, t1.study_id,t1.created_at ,
          t2.form_id,  t2.activity_name, QRE.query_response_id,
          t2.country_id,t2.country_name,t2.site_id,t2.site_name,t2.study_name, 
          CASE
              WHEN QRE.query_response_id is null AND t1.status = 'OPEN' THEN 'OPEN'
              WHEN QRE.query_response_id is not null AND t1.status = 'OPEN' THEN 'RESPONDED NOT CLOSED'
              WHEN t1.status = 'CLOSE' THEN 'CLOSE'
              END AS new_status,
          CASE
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) < 2 THEN '0-1 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 1 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 5 THEN '>1-4 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 4 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 11 THEN '>4-10 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 10 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 21 THEN '>10-20 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 20 THEN '>20 days'
              END AS Bucket_type
          from
          (
          select id as query_id, status, response_id, study_id,created_at
          from research_response.edc_query
          where study_id = ?
          ) t1
          LEFT JOIN
          (select 
          QR.response_id, QR.form_id, QR.study_id, QF.activity_name, QR.participant_id,
          PSC.country_id,PSC.country_name,PSC.site_id,PSC.site_name,PSC.study_name
          from
          (select id as response_id, form_id, study_id, participant_id,submitted_at
          from 
          research_response.edc_response
          where study_id = ?) QR
          LEFT JOIN (select 
          id,study_id,created_at, name activity_name
          from
          research_response.edc_form
          where study_id = ?) QF ON QR.form_id = QF.id
          LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id and
                                                                      QR.study_id = PSC.study_id
          ) t2 ON t1.response_id = t2.response_id
          LEFT JOIN (
          select 
          EQ.id query_response_id
          from 
          research_response.edc_query EQ
          LEFT JOIN research_response.edc_query_comment EQC ON EQ.id = EQC.edc_query_id 
          where  
          EQC.created_by != EQ.created_by
          and EQ.study_id = ?
          group by query_response_id
          ) QRE ON t1.query_id = QRE.query_response_id
      
          ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
          ${params.siteId ? `t2.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
          ${params.fromDate ? `(t1.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
          ${params.participantsIds ? `t2.participant_id in (${participantsIds}) ` : ''} 
          ) t1) t2 , (
          select '0-1 days' Bucket_type union select '>1-4 days'
          union select '>4-10 days' union select '>10-20 days' union select '>20 days'
          ) t1
          where t2.activity_name is not null and  t3.site_name is not null
          ) t1
          LEFT JOIN (
          select
          site_id, form_id,site_name, activity_name, Bucket_type,
          COUNT(CASE WHEN t1.new_status = 'OPEN' THEN 1 END) AS 'n_queries_open',
          COUNT(CASE WHEN t1.new_status = 'RESPONDED NOT CLOSED' THEN 1 END) AS 'n_responded_not_closed',
          COUNT(CASE WHEN t1.new_status = 'CLOSE' THEN 1 END) AS 'n_closed_query'
          from
          (
          select
          t1.query_id,t2.participant_id, t1.status old_status, t1.response_id, t1.study_id,t1.created_at ,
          t2.form_id,  t2.activity_name, QRE.query_response_id,
          t2.country_id,t2.country_name,t2.site_id,t2.site_name,t2.study_name, 
          CASE
              WHEN QRE.query_response_id is null AND t1.status = 'OPEN' THEN 'OPEN'
              WHEN QRE.query_response_id is not null AND t1.status = 'OPEN' THEN 'RESPONDED NOT CLOSED'
              WHEN t1.status = 'CLOSE' THEN 'CLOSE'
              END AS new_status,
          CASE
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) < 2 THEN '0-1 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 1 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 5 THEN '>1-4 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 4 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 11 THEN '>4-10 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 10 AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 21 THEN '>10-20 days'
              WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 20 THEN '>20 days'
              END AS Bucket_type
          from
          (
          select id as query_id, status, response_id, study_id,created_at
          from research_response.edc_query
          where study_id = ?
          ) t1
          LEFT JOIN
          (select 
          QR.response_id, QR.form_id, QR.study_id, QF.activity_name, QR.participant_id,
          PSC.country_id,PSC.country_name,PSC.site_id,PSC.site_name,PSC.study_name
          from
          (select id as response_id, form_id, study_id, participant_id,submitted_at
          from 
          research_response.edc_response
          where study_id = ?) QR
          LEFT JOIN (select 
          id,study_id,created_at, name activity_name
          from
          research_response.edc_form
          where study_id = ?) QF ON QR.form_id = QF.id
          LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id and
                                                                      QR.study_id = PSC.study_id
          ) t2 ON t1.response_id = t2.response_id
          LEFT JOIN (
          select 
          EQ.id query_response_id
          from 
          research_response.edc_query EQ
          LEFT JOIN research_response.edc_query_comment EQC ON EQ.id = EQC.edc_query_id 
          where  
          EQC.created_by != EQ.created_by
          and EQ.study_id = ?
          group by query_response_id
          ) QRE ON t1.query_id = QRE.query_response_id
          ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
          ${params.siteId ? `t2.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
          ${params.fromDate ? `(t1.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
          ${params.participantsIds ? `t2.participant_id in (${participantsIds}) ` : ''} 
          ) t1
          group by site_id, form_id, Bucket_type
          ) t2 on t1.Bucket_type = t2.Bucket_type and
                                        t1.site_name = t2.site_name and
                                        t1.activity_name = t2.activity_name
          union all
          select 
            t1.country_name as countryName, 
            t1.site_name as siteName, 
            t1.activity_name as activityName, 
            t1.Bucket_type as bucketType, 
            IFNULL(t2.n_queries_open, 0) n_queries_open, 
            IFNULL(t2.n_responded_not_closed, 0) n_responded_not_closed, 
            IFNULL(t2.n_closed_query, 0) n_closed_query 
          from 
            (
              select 
                * 
              from 
                (
                  select 
                    distinct(site_name) site_name, 
                    country_name 
                  from 
                    (
                      select 
                        t1.query_id, 
                        t2.participant_id, 
                        t1.status old_status, 
                        t1.response_id, 
                        t1.study_id, 
                        t1.created_at, 
                        t2.form_id, 
                        t2.activity_name, 
                        QRE.query_response_id, 
                        t2.country_id, 
                        t2.country_name, 
                        t2.site_id, 
                        t2.site_name, 
                        t2.study_name, 
                        CASE WHEN QRE.query_response_id is null 
                        AND t1.status = 'OPEN' THEN 'OPEN' WHEN QRE.query_response_id is not null 
                        AND t1.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN t1.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
                        CASE WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) < 2 THEN '0-1 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 1 
                        AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 5 THEN '>1-4 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 4 
                        AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 11 THEN '>4-10 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 10 
                        AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 21 THEN '>10-20 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 20 THEN '>20 days' END AS Bucket_type 
                      from 
                        (
                          select 
                            id as query_id, 
                            status, 
                            response_id, 
                            study_id, 
                            created_at 
                          from 
                            research_response.edc_query 
                          where 
                            study_id = ?
                        ) t1 
                        LEFT JOIN (
                          select 
                            QR.response_id, 
                            QR.form_id, 
                            QR.study_id, 
                            QF.activity_name, 
                            QR.participant_id, 
                            PSC.country_id, 
                            PSC.country_name, 
                            PSC.site_id, 
                            PSC.site_name, 
                            PSC.study_name 
                          from 
                            (
                              select 
                                id as response_id, 
                                edc_unsch_pack_id as form_id, 
                                study_id, 
                                participant_id, 
                                submitted_at 
                              from 
                                research_response.edc_unscheduled_packet_response 
                              where 
                                study_id = ?
                            ) QR 
                            LEFT JOIN (
                              select 
                                id, 
                                study_id, 
                                created_at, 
                                packet_name as activity_name 
                              from 
                                research_response.edc_unscheduled_packet 
                              where 
                                study_id = ?
                            ) QF ON QR.form_id = QF.id 
                            LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id 
                            and QR.study_id = PSC.study_id
                        ) t2 ON t1.response_id = t2.response_id 
                        LEFT JOIN (
                          select 
                            EQ.id query_response_id 
                          from 
                            research_response.edc_query EQ 
                            LEFT JOIN research_response.edc_query_comment EQC ON EQ.id = EQC.edc_query_id 
                          where 
                            EQC.created_by != EQ.created_by 
                            and EQ.study_id = ? 
                          group by 
                            query_response_id
                        ) QRE ON t1.query_id = QRE.query_response_id ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : '' } ${params.siteId ? `t2.site_id = '${params.siteId}' ` : '' } ${(params.siteId && params.fromDate) || (
                          params.siteId && params.participantsIds
                        ) ? 'AND ' : '' } ${params.fromDate ? `(t1.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : '' } ${params.fromDate && params.participantsIds ? 'AND ' : '' } ${params.participantsIds ? `t2.participant_id in (${participantsIds}) ` : '' }
                    ) t1
                ) t3, 
                (
                  select 
                    distinct(activity_name) activity_name 
                  from 
                    (
                      select 
                        t1.query_id, 
                        t2.participant_id, 
                        t1.status old_status, 
                        t1.response_id, 
                        t1.study_id, 
                        t1.created_at, 
                        t2.form_id, 
                        t2.activity_name, 
                        QRE.query_response_id, 
                        t2.country_id, 
                        t2.country_name, 
                        t2.site_id, 
                        t2.site_name, 
                        t2.study_name, 
                        CASE WHEN QRE.query_response_id is null 
                        AND t1.status = 'OPEN' THEN 'OPEN' WHEN QRE.query_response_id is not null 
                        AND t1.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN t1.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
                        CASE WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) < 2 THEN '0-1 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 1 
                        AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 5 THEN '>1-4 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 4 
                        AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 11 THEN '>4-10 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 10 
                        AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 21 THEN '>10-20 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 20 THEN '>20 days' END AS Bucket_type 
                      from 
                        (
                          select 
                            id as query_id, 
                            status, 
                            response_id, 
                            study_id, 
                            created_at 
                          from 
                            research_response.edc_query 
                          where 
                            study_id = ?
                        ) t1 
                        LEFT JOIN (
                          select 
                            QR.response_id, 
                            QR.form_id, 
                            QR.study_id, 
                            QF.activity_name, 
                            QR.participant_id, 
                            PSC.country_id, 
                            PSC.country_name, 
                            PSC.site_id, 
                            PSC.site_name, 
                            PSC.study_name 
                          from 
                            (
                              select 
                                id as response_id, 
                                edc_unsch_pack_id as form_id, 
                                study_id, 
                                participant_id, 
                                submitted_at 
                              from 
                                research_response.edc_unscheduled_packet_response 
                              where 
                                study_id = ?
                            ) QR 
                            LEFT JOIN (
                              select 
                                id, 
                                study_id, 
                                created_at, 
                                packet_name as activity_name 
                              from 
                                research_response.edc_unscheduled_packet 
                              where 
                                study_id = ?
                            ) QF ON QR.form_id = QF.id 
                            LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id 
                            and QR.study_id = PSC.study_id
                        ) t2 ON t1.response_id = t2.response_id 
                        LEFT JOIN (
                          select 
                            EQ.id query_response_id 
                          from 
                            research_response.edc_query EQ 
                            LEFT JOIN research_response.edc_query_comment EQC ON EQ.id = EQC.edc_query_id 
                          where 
                            EQC.created_by != EQ.created_by 
                            and EQ.study_id = ? 
                          group by 
                            query_response_id
                        ) QRE ON t1.query_id = QRE.query_response_id ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : '' } ${params.siteId ? `t2.site_id = '${params.siteId}' ` : '' } ${(params.siteId && params.fromDate) || (
                          params.siteId && params.participantsIds
                        ) ? 'AND ' : '' } ${params.fromDate ? `(t1.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : '' } ${params.fromDate && params.participantsIds ? 'AND ' : '' } ${params.participantsIds ? `t2.participant_id in (${participantsIds}) ` : '' }
                    ) t1
                ) t2, 
                (
                  select 
                    '0-1 days' Bucket_type 
                  union 
                  select 
                    '>1-4 days' 
                  union 
                  select 
                    '>4-10 days' 
                  union 
                  select 
                    '>10-20 days' 
                  union 
                  select 
                    '>20 days'
                ) t1 
              where 
                t2.activity_name is not null 
                and t3.site_name is not null
            ) t1 
            LEFT JOIN (
              select 
                site_id, 
                form_id, 
                site_name, 
                activity_name, 
                Bucket_type, 
                COUNT(
                  CASE WHEN t1.new_status = 'OPEN' THEN 1 END
                ) AS 'n_queries_open', 
                COUNT(
                  CASE WHEN t1.new_status = 'RESPONDED NOT CLOSED' THEN 1 END
                ) AS 'n_responded_not_closed', 
                COUNT(
                  CASE WHEN t1.new_status = 'CLOSE' THEN 1 END
                ) AS 'n_closed_query' 
              from 
                (
                  select 
                    t1.query_id, 
                    t2.participant_id, 
                    t1.status old_status, 
                    t1.response_id, 
                    t1.study_id, 
                    t1.created_at, 
                    t2.form_id, 
                    t2.activity_name, 
                    QRE.query_response_id, 
                    t2.country_id, 
                    t2.country_name, 
                    t2.site_id, 
                    t2.site_name, 
                    t2.study_name, 
                    CASE WHEN QRE.query_response_id is null 
                    AND t1.status = 'OPEN' THEN 'OPEN' WHEN QRE.query_response_id is not null 
                    AND t1.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN t1.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
                    CASE WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) < 2 THEN '0-1 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 1 
                    AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 5 THEN '>1-4 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 4 
                    AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 11 THEN '>4-10 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 10 
                    AND TIMESTAMPDIFF (day, t1.created_at, NOW()) < 21 THEN '>10-20 days' WHEN TIMESTAMPDIFF (day, t1.created_at, NOW()) > 20 THEN '>20 days' END AS Bucket_type 
                  from 
                    (
                      select 
                        id as query_id, 
                        status, 
                        response_id, 
                        study_id, 
                        created_at 
                      from 
                        research_response.edc_query 
                      where 
                        study_id = ?
                    ) t1 
                    LEFT JOIN (
                      select 
                        QR.response_id, 
                        QR.form_id, 
                        QR.study_id, 
                        QF.activity_name, 
                        QR.participant_id, 
                        PSC.country_id, 
                        PSC.country_name, 
                        PSC.site_id, 
                        PSC.site_name, 
                        PSC.study_name 
                      from 
                        (
                          select 
                            id as response_id, 
                            edc_unsch_pack_id as form_id, 
                            study_id, 
                            participant_id, 
                            submitted_at 
                          from 
                            research_response.edc_unscheduled_packet_response 
                          where 
                            study_id = ?
                        ) QR 
                        LEFT JOIN (
                          select 
                            id, 
                            study_id, 
                            created_at, 
                            packet_name as activity_name 
                          from 
                            research_response.edc_unscheduled_packet 
                          where 
                            study_id = ?
                        ) QF ON QR.form_id = QF.id 
                        LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id 
                        and QR.study_id = PSC.study_id
                    ) t2 ON t1.response_id = t2.response_id 
                    LEFT JOIN (
                      select 
                        EQ.id query_response_id 
                      from 
                        research_response.edc_query EQ 
                        LEFT JOIN research_response.edc_query_comment EQC ON EQ.id = EQC.edc_query_id 
                      where 
                        EQC.created_by != EQ.created_by 
                        and EQ.study_id = ? 
                      group by 
                        query_response_id
                    ) QRE ON t1.query_id = QRE.query_response_id ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : '' } ${params.siteId ? `t2.site_id = '${params.siteId}' ` : '' } ${(params.siteId && params.fromDate) || (
                      params.siteId && params.participantsIds
                    ) ? 'AND ' : '' } ${params.fromDate ? `(t1.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : '' } ${params.fromDate && params.participantsIds ? 'AND ' : '' } ${params.participantsIds ? `t2.participant_id in (${participantsIds}) ` : '' }
                ) t1 
              group by 
                site_id, 
                form_id, 
                Bucket_type
            ) t2 on t1.Bucket_type = t2.Bucket_type 
            and t1.site_name = t2.site_name 
            and t1.activity_name = t2.activity_name                              
          `
          console.log(`getQueriesData query SQL ${JSON.stringify(params)} \n${querySql}`);
          const [data] = await dbConnectionPool.query(querySql, bindingParams)
          dbConnectionPool.end();
          return data;
        } catch (error) {
          dbConnectionPool.end();
          console.log('Error in function getQueriesData:', error);
          throw error;
        }
    }

    async getActivitiesByTimeDIffData(params){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
      try {
        const bindingParams = []
        for (let index = 0; index < 3; index++) {
          bindingParams.push(params.studyId)
        }
        let participantsIds = null;
        if (params.participantsIds) {
          participantsIds = convertArrayToString(params.participantsIds);
        }
        let querySql = `
        select
          study_id, country_id, country_name as countryName, site_id, site_name as siteName, activity_name as activityName,(form_created_at),data_entry_at,
          IFNULL((AVG(TIMESTAMPDIFF(SECOND, form_created_at, data_entry_at))), 0) as secondEntryCompleted,
          IFNULL((AVG(TIMESTAMPDIFF(SECOND, form_created_at, approve_date))), 0) as secondApproved,
          IFNULL((AVG(TIMESTAMPDIFF(SECOND, form_created_at, verify_date))), 0) as secondVerified,
          IFNULL((AVG(TIMESTAMPDIFF(SECOND, form_created_at, signed_at))), 0) as secondSigned
          from
          (
          select
          QR.response_id, QR.form_id, QR.study_id, QR.participant_id, 
          QF.activity_name,PSC.enrollment_date,
          QF.created_at as form_created_at,QR.data_entry_at,QFE.approve_date,QFE.verify_date,QFE.signed_at,
          PSC.country_id,PSC.country_name,PSC.site_id,PSC.site_name,PSC.study_name
          from
          (
          select ER.id as response_id, ER.form_id, ER.study_id, ER.participant_id, ER.submitted_at data_entry_at
          from 
          research_response.edc_response ER
          INNER JOIN
          (select form_id,participant_id, min(submitted_at) submitted_at
          from 
          research_response.edc_response
          where study_id = ?
          group by form_id,participant_id) AGG1 ON ER.form_id = AGG1.form_id and
                                                  ER.participant_id = AGG1.participant_id and
                                                  ER.submitted_at = AGG1.submitted_at
          where study_id = ?
          ) QR
          LEFT JOIN (
          select 
          t1.response_id, 
          case
          when t4.not_approved_at > t3.approved_at then null 
          else t3.approved_at
          end as approve_date,
          case when t6.not_verified_at > t5.verified_at then null
          else t5.verified_at
          end as verify_date,
          t7.signed_at
          from 
          (select distinct(response_id) from research_response.edc_response_event) t1
          LEFT JOIN (
          select response_id, max(created_at) as approved_at
          from 
          (select
          *
          from research_response.edc_response_event
          where event_type = 'APPROVED') P
          group by response_id
          ) t3 ON t1.response_id = t3.response_id
          LEFT JOIN (
          select response_id, max(created_at) as not_approved_at
          from 
          (select
          *
          from research_response.edc_response_event
          where event_type = 'NOT_APPROVED') P
          group by response_id
          ) t4 ON t1.response_id = t4.response_id
          LEFT JOIN (
          select response_id, max(created_at) as verified_at
          from 
          (select
          *
          from research_response.edc_response_event
          where event_type = 'VERIFIED') P
          group by response_id
          ) t5 ON t1.response_id = t5.response_id
          LEFT JOIN (
          select response_id, max(created_at) as not_verified_at
          from 
          (select
          *
          from research_response.edc_response_event
          where event_type = 'NOT_VERIFIED') P
          group by response_id
          ) t6 ON t1.response_id = t6.response_id
          LEFT JOIN (
          select response_id, max(created_at) as signed_at
          from 
          (select
          *
          from research_response.edc_response_event
          where event_type = 'SIGNEDOFF') P
          group by response_id
          ) t7 ON t1.response_id = t7.response_id
          ) QFE ON QR.response_id = QFE.response_id
          LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id and
                                                                      QR.study_id = PSC.study_id
          LEFT JOIN (
          select 
          id,study_id,created_at, name activity_name
          from
          research_response.edc_form
          where study_id = ?
          ) QF ON QR.form_id = QF.id and 
                                        QR.study_id = QF.study_id
          ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
          ${params.siteId ? `PSC.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
          ${params.fromDate ? `(QR.data_entry_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
          ${params.participantsIds ? `QR.participant_id in (${participantsIds}) ` : ''} 
          ) t1
          group by site_id, country_id, form_id
        `
        console.log(`getActivitiesByTimeDIffData query SQL ${JSON.stringify(params)} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function getActivitiesByTimeDIffData:', error);
        throw error;
      }
  }

  async getActivitiesData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      for (let index = 0; index < 15; index++) {
        bindingParams.push(params.studyId)
      }
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      let querySql = `
      SELECT study_id, country_id, country_name 
        countryName, site_id, site_name 
        siteName, activity_name 
        activityName, n_entry 
        AS totalEntry, n_approve 
        AS totalApproved, n_verify 
        AS totalVerified, n_signed 
        AS totalSigned, ( n_entry / (SELECT Count(DISTINCT( participant_id )) FROM (SELECT QR.response_id, QR.form_id, QR.study_id, QR.participant_id, QF.activity_name, PSC.enrollment_date, QF.created_at AS form_created_at, QR.data_entry_at, QFE.approve_date, QFE.verify_date, QFE.signed_at, PSC.country_id, PSC.country_name, PSC.site_id, PSC.site_name, PSC.study_name 
        FROM (SELECT ER.id  AS response_id,  ER.form_id,  ER.study_id,  ER.participant_id,  ER.submitted_at data_entry_at 
        FROM research_response.edc_response ER 
        INNER JOIN (SELECT 
        form_id,  participant_id,  Min(submitted_at) submitted_at 
          FROM  research_response.edc_response 
          WHERE study_id = ? 
          GROUP  BY form_id, participant_id) AGG1 
        ON ER.form_id = AGG1.form_id 
        AND ER.participant_id =  AGG1.participant_id 
        AND ER.submitted_at = AGG1.submitted_at 
        WHERE study_id = ?) QR 
        LEFT JOIN (SELECT t1.response_id, CASE 
          WHEN t4.not_approved_at > t3.approved_at 
          THEN NULL 
          ELSE t3.approved_at 
          END AS approve_date, CASE 
          WHEN t6.not_verified_at > t5.verified_at 
          THEN NULL 
          ELSE t5.verified_at 
          END AS verify_date, t7.signed_at 
          FROM (SELECT DISTINCT( 
          response_id )  FROM research_response.edc_response_event)  t1 
        LEFT JOIN (SELECT 
        response_id,  Max(created_at) AS  approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'APPROVED') P 
        GROUP  BY response_id) t3 
        ON t1.response_id = t3.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_APPROVED') P 
        GROUP  BY response_id) t4 
        ON t1.response_id = t4.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'VERIFIED') P 
        GROUP  BY response_id) t5 
        ON t1.response_id = t5.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_VERIFIED') P 
        GROUP  BY response_id) t6 
        ON t1.response_id = t6.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS signed_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'SIGNEDOFF') P 
        GROUP  BY response_id) t7 
        ON t1.response_id = t7.response_id) QFE 
        ON QR.response_id = QFE.response_id 
        LEFT JOIN research_analytics.participant_site_country PSC 
        ON QR.participant_id = PSC.participant_id 
        AND QR.study_id = PSC.study_id 
        LEFT JOIN (SELECT id, study_id, created_at, NAME activity_name 
        FROM research_response.edc_form 
        WHERE study_id = ?) QF 
        ON QR.form_id = QF.id 
        AND QR.study_id = QF.study_id 
        WHERE QR.data_entry_at BETWEEN '1969-05-29' AND '2021-10-30') t1) * 100 ) AS percentageEntry, ( n_approve / (SELECT Count(DISTINCT( participant_id )) FROM (SELECT QR.response_id, QR.form_id, QR.study_id, QR.participant_id, QF.activity_name, PSC.enrollment_date, QF.created_at AS form_created_at, QR.data_entry_at, QFE.approve_date, QFE.verify_date, QFE.signed_at, PSC.country_id, PSC.country_name, PSC.site_id, PSC.site_name, PSC.study_name 
        FROM (SELECT ER.id  AS response_id, ER.form_id, ER.study_id, ER.participant_id, ER.submitted_at data_entry_at 
        FROM research_response.edc_response ER 
        INNER JOIN (SELECT 
        form_id, participant_id, Min(submitted_at) submitted_at 
        FROM research_response.edc_response 
        WHERE study_id = ? 
        GROUP  BY form_id, participant_id) AGG1 
        ON ER.form_id = AGG1.form_id 
        AND ER.participant_id = AGG1.participant_id 
        AND ER.submitted_at = AGG1.submitted_at 
        WHERE study_id = ?) QR 
        LEFT JOIN (SELECT t1.response_id,  CASE 
        WHEN t4.not_approved_at >  t3.approved_at 
        THEN NULL 
        ELSE t3.approved_at 
        END AS approve_date,  CASE 
        WHEN t6.not_verified_at >  t5.verified_at 
        THEN NULL 
        ELSE t5.verified_at 
        END AS verify_date,  t7.signed_at 
        FROM (SELECT DISTINCT( response_id ) FROM research_response.edc_response_event)  t1 
        LEFT JOIN (SELECT 
        response_id,  Max(created_at) AS  approved_at 
        FROM (SELECT *  FROM research_response.edc_response_event 
        WHERE event_type = 'APPROVED') P 
        GROUP  BY response_id) t3 
        ON t1.response_id = t3.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_APPROVED') P 
        GROUP  BY response_id) t4 
        ON t1.response_id = t4.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'VERIFIED') P 
        GROUP  BY response_id) t5 
        ON t1.response_id = t5.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_VERIFIED') P 
        GROUP  BY response_id) t6 
        ON t1.response_id = t6.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS signed_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'SIGNEDOFF') P 
        GROUP  BY response_id) t7 
        ON t1.response_id = t7.response_id) QFE 
        ON QR.response_id = QFE.response_id 
        LEFT JOIN research_analytics.participant_site_country PSC 
        ON QR.participant_id = PSC.participant_id 
        AND QR.study_id = PSC.study_id 
        LEFT JOIN (SELECT id, study_id, created_at, NAME activity_name 
        FROM research_response.edc_form 
        WHERE study_id = ?) QF 
        ON QR.form_id = QF.id 
        AND QR.study_id = QF.study_id 
        WHERE QR.data_entry_at BETWEEN '1969-05-29' AND '2021-10-30') t1) * 100 ) AS percentageApprove, ( n_verify / (SELECT Count(DISTINCT( participant_id )) FROM (SELECT QR.response_id, QR.form_id, QR.study_id, QR.participant_id, QF.activity_name, PSC.enrollment_date, QF.created_at AS form_created_at, QR.data_entry_at, QFE.approve_date, QFE.verify_date, QFE.signed_at, PSC.country_id, PSC.country_name, PSC.site_id, PSC.site_name, PSC.study_name 
        FROM (SELECT ER.id  AS response_id, ER.form_id, ER.study_id, ER.participant_id, ER.submitted_at data_entry_at 
        FROM research_response.edc_response ER 
        INNER JOIN (SELECT 
        form_id, participant_id, Min(submitted_at) submitted_at 
          FROM research_response.edc_response 
          WHERE study_id = ? 
        GROUP  BY form_id,  participant_id) AGG1 
        ON ER.form_id = AGG1.form_id 
        AND ER.participant_id = AGG1.participant_id 
        AND ER.submitted_at = AGG1.submitted_at 
        WHERE study_id = ?) QR 
        LEFT JOIN (SELECT t1.response_id, CASE 
          WHEN t4.not_approved_at > t3.approved_at 
          THEN NULL 
          ELSE t3.approved_at 
          END AS approve_date, CASE 
          WHEN t6.not_verified_at > t5.verified_at 
          THEN NULL 
          ELSE t5.verified_at 
          END AS verify_date, t7.signed_at 
          FROM (SELECT DISTINCT( response_id )  FROM  research_response.edc_response_event) t1 
          LEFT JOIN (SELECT 
          response_id, Max(created_at) AS approved_at 
          FROM (SELECT *  FROM research_response.edc_response_event 
        WHERE event_type = 'APPROVED') P 
        GROUP  BY response_id) t3 
        ON t1.response_id = t3.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_APPROVED') P 
        GROUP  BY response_id) t4 
        ON t1.response_id = t4.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'VERIFIED') P 
        GROUP  BY response_id) t5 
        ON t1.response_id = t5.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_VERIFIED') P 
        GROUP  BY response_id) t6 
        ON t1.response_id = t6.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS signed_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'SIGNEDOFF') P 
        GROUP  BY response_id) t7 
        ON t1.response_id = t7.response_id) QFE 
        ON QR.response_id = QFE.response_id 
        LEFT JOIN research_analytics.participant_site_country PSC 
        ON QR.participant_id = PSC.participant_id 
        AND QR.study_id = PSC.study_id 
        LEFT JOIN (SELECT id, study_id, created_at, NAME activity_name 
        FROM research_response.edc_form 
        WHERE study_id = ?) QF 
        ON QR.form_id = QF.id 
        AND QR.study_id = QF.study_id 
        WHERE QR.data_entry_at BETWEEN '1969-05-29' AND '2021-10-30') t1) * 100 ) AS percentageVerify, ( n_signed / (SELECT Count(DISTINCT( participant_id )) FROM (SELECT QR.response_id, QR.form_id, QR.study_id, QR.participant_id, QF.activity_name, PSC.enrollment_date, QF.created_at AS form_created_at, QR.data_entry_at, QFE.approve_date, QFE.verify_date, QFE.signed_at, PSC.country_id, PSC.country_name, PSC.site_id, PSC.site_name, PSC.study_name 
        FROM (SELECT ER.id  AS response_id, ER.form_id, ER.study_id, ER.participant_id, ER.submitted_at data_entry_at 
        FROM research_response.edc_response ER 
        INNER JOIN (SELECT 
        form_id, participant_id, Min(submitted_at) submitted_at 
          FROM research_response.edc_response 
          WHERE study_id = ? 
        GROUP  BY form_id,  participant_id) AGG1 
        ON ER.form_id = AGG1.form_id 
        AND ER.participant_id = AGG1.participant_id 
        AND ER.submitted_at = AGG1.submitted_at 
        WHERE study_id = ?) QR 
        LEFT JOIN (SELECT t1.response_id, CASE 
          WHEN t4.not_approved_at > t3.approved_at 
          THEN NULL 
          ELSE t3.approved_at 
          END AS approve_date, CASE 
          WHEN t6.not_verified_at > t5.verified_at 
          THEN NULL 
          ELSE t5.verified_at 
          END AS verify_date, t7.signed_at 
          FROM (SELECT DISTINCT( response_id )  FROM  research_response.edc_response_event) t1 
          LEFT JOIN (SELECT 
          response_id, Max(created_at) AS approved_at 
          FROM (SELECT *  FROM research_response.edc_response_event 
        WHERE event_type = 'APPROVED') P 
        GROUP  BY response_id) t3 
        ON t1.response_id = t3.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_APPROVED') P 
        GROUP  BY response_id) t4 
        ON t1.response_id = t4.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'VERIFIED') P 
        GROUP  BY response_id) t5 
        ON t1.response_id = t5.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS not_verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'NOT_VERIFIED') P 
        GROUP  BY response_id) t6 
        ON t1.response_id = t6.response_id 
        LEFT JOIN (SELECT response_id, Max(created_at) AS signed_at 
        FROM (SELECT * FROM research_response.edc_response_event 
        WHERE event_type = 'SIGNEDOFF') P 
        GROUP  BY response_id) t7 
        ON t1.response_id = t7.response_id) QFE 
        ON QR.response_id = QFE.response_id 
        LEFT JOIN research_analytics.participant_site_country PSC 
        ON QR.participant_id = PSC.participant_id 
        AND QR.study_id = PSC.study_id 
        LEFT JOIN (SELECT id, study_id, created_at, NAME activity_name 
        FROM research_response.edc_form 
        WHERE study_id = ?) QF 
        ON QR.form_id = QF.id 
        AND QR.study_id = QF.study_id 
        WHERE QR.data_entry_at BETWEEN '1969-05-29' AND '2021-10-30') t1) * 100 ) AS percentageSigned 
        FROM (SELECT study_id,  country_id,  country_name,  site_id,  site_name,  activity_name,  Count(CASE 
          WHEN Dayname(data_entry_at) IS NOT NULL THEN 1 
        END) AS n_entry,  Count(CASE 
          WHEN Dayname(approve_date) IS NOT NULL THEN 1 
        END) AS n_approve,  Count(CASE 
          WHEN Dayname(verify_date) IS NOT NULL THEN 1 
        END) AS n_verify,  Count(CASE 
          WHEN Dayname(signed_at) IS NOT NULL THEN 1 
        END) AS n_signed 
        FROM (SELECT QR.response_id, QR.form_id, QR.study_id, QR.participant_id, QF.activity_name, PSC.enrollment_date, QF.created_at AS form_created_at, QR.data_entry_at, QFE.approve_date, QFE.verify_date, QFE.signed_at, PSC.country_id, PSC.country_name, PSC.site_id, PSC.site_name, PSC.study_name 
          FROM (SELECT ER.id  AS response_id,  ER.form_id,  ER.study_id,  ER.participant_id,  ER.submitted_at data_entry_at 
        FROM research_response.edc_response ER 
        INNER JOIN (SELECT form_id,  participant_id,  Min(submitted_at) submitted_at 
        FROM research_response.edc_response 
        WHERE  study_id = ? 
        GROUP  BY form_id, participant_id) AGG1 
          ON ER.form_id = AGG1.form_id 
        AND ER.participant_id = AGG1.participant_id 
        AND ER.submitted_at = AGG1.submitted_at 
        WHERE study_id = ? 
          ) QR 
          LEFT JOIN (SELECT t1.response_id, CASE 
        WHEN t4.not_approved_at > t3.approved_at 
          THEN NULL 
        ELSE t3.approved_at 
          END AS approve_date, CASE 
        WHEN t6.not_verified_at > t5.verified_at 
          THEN NULL 
        ELSE t5.verified_at 
          END AS verify_date, t7.signed_at 
          FROM (SELECT DISTINCT( response_id )  FROM  research_response.edc_response_event) t1 
          LEFT JOIN (SELECT response_id,  Max(created_at) AS  approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
          WHERE event_type = 'APPROVED') P 
        GROUP  BY response_id) t3 
        ON t1.response_id = t3.response_id 
        LEFT JOIN (SELECT response_id,  Max(created_at) AS  not_approved_at 
        FROM (SELECT * FROM research_response.edc_response_event 
          WHERE event_type = 'NOT_APPROVED')  P 
        GROUP  BY response_id) t4 
        ON t1.response_id = t4.response_id 
        LEFT JOIN (SELECT response_id,  Max(created_at) AS  verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
          WHERE event_type = 'VERIFIED') P 
        GROUP  BY response_id) t5 
        ON t1.response_id = t5.response_id 
        LEFT JOIN (SELECT response_id,  Max(created_at) AS  not_verified_at 
        FROM (SELECT * FROM research_response.edc_response_event 
          WHERE event_type = 'NOT_VERIFIED')  P 
        GROUP  BY response_id) t6 
        ON t1.response_id = t6.response_id 
        LEFT JOIN (SELECT response_id,  Max(created_at) AS signed_at 
        FROM (SELECT * FROM research_response.edc_response_event 
          WHERE event_type = 'SIGNEDOFF') P 
        GROUP  BY response_id) t7 
        ON t1.response_id = t7.response_id) QFE 
        ON QR.response_id = QFE.response_id 
        LEFT JOIN research_analytics.participant_site_country PSC 
        ON QR.participant_id = PSC.participant_id 
        AND QR.study_id = PSC.study_id 
        LEFT JOIN (SELECT id, study_id, created_at, NAME activity_name 
        FROM research_response.edc_form 
        WHERE study_id = ?) QF 
        ON QR.form_id = QF.id 
        AND QR.study_id = QF.study_id 
        ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
        ${params.siteId ? `PSC.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
        ${params.fromDate ? `(QR.data_entry_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
        ${params.participantsIds ? `QR.participant_id in (${participantsIds}) ` : ''} 
        ) t1 
        GROUP  BY site_id, country_id, activity_name) t1 
      `
      console.log(`getActivitiesData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getActivitiesData:', error);
      throw error;
    }
  }

  async getTotalDataChange(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      let sitesIds = null;
      let countriesIds = null;
      for (let index = 0; index < 4; index++) {
        bindingParams.push(params.studyId)
      }

      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }

      if (params.siteId && Array.isArray(params.siteId)) {
        sitesIds = convertArrayToString(params.siteId);
      } else if (params.siteId && typeof params.siteId === 'string') {
        sitesIds = "'" + params.siteId + "'"
      }

      if (params.countriesIds) {
        countriesIds = convertArrayToString(params.countriesIds);
      }

      let querySql = `select
      t1.site_id,
      t1.country_id,
      t1.site_name as siteName,
      t1.country_name as countryName,
      t1.total_data_change,
      t2.total_data_change total_data_change_prev1wk
    from
      (
        select
          site_id,
          country_id,
          site_name,
          country_name,
          SUM(total_data_change) total_data_change
        from
          (
            select
              *
            from
              (
                select
                  site_id,
                  country_id,
                  site_name,
                  country_name,
                  count(*) total_data_change
                from
                  (
                    select
                      EUV.id,
                      EUV.site_id,
                      PSC.site_name,
                      EU.participant_id,
                      EU.study_id,
                      PSC.country_id,
                      PSC.country_name  
                    from
                      research_response.edc_response_version EUV
                      LEFT JOIN research_response.edc_response EU ON EUV.edc_response_id = EU.id
                      LEFT JOIN research_analytics.participant_site_country PSC on EU.participant_id = PSC.participant_id
                    where
                      EUV.version > 1
                      and EUV.state = 'SUBMITTED'
                      and EU.study_id = ? and EUV.created_at <= DATE_ADD(curdate(), interval 1 day) 
                      ${params.siteId ? `and EUV.site_id in (${sitesIds}) ` : ''} 
                      ${params.participantsIds ? ` and EU.participant_id in (${participantsIds}) ` : ''} 
                      ${params.countriesIds ? ` and PSC.country_id in (${countriesIds}) ` : ''} 
                      ${params.fromDate ? ` and EUV.created_at between '${params.fromDate}' and '${params.toDate}'` : ''}
                  ) t1
                group by
                  site_id,
                  country_id
              ) QD
            UNION ALL
            select
              *
            from
              (
                select
                  site_id,
                  country_id,
                  site_name,
                  country_name,
                  count(*) total_data_change
                from
                  (
                    select
                      EUV.id,
                      EUV.site_id,
                      PSC.site_name,
                      EU.participant_id,
                      EU.study_id,
                      PSC.country_id,
                      PSC.country_name
                    from
                      research_response.edc_unscheduled_packet_response_version EUV
                      LEFT JOIN research_response.edc_unscheduled_packet_response EU ON EUV.edc_unscheduled_packet_response_id = EU.id
                      LEFT JOIN research_analytics.participant_site_country PSC on EU.participant_id = PSC.participant_id
                    where
                      EUV.version > 1
                      and EUV.state = 'SUBMITTED'
                      and EU.study_id = ?
                      and EUV.created_at <= DATE_ADD(curdate(), interval 1 day) 
                      ${params.siteId ? `and EUV.site_id in (${sitesIds}) ` : ''}
                      ${params.participantsIds ? ` and EU.participant_id in (${participantsIds}) ` : ''} 
                      ${params.countriesIds ? ` and PSC.country_id in (${countriesIds}) ` : ''} 
                      ${params.fromDate ? ` and EUV.created_at between '${params.fromDate}' and '${params.toDate}'` : ''}
                  ) t1
                group by
                  site_id,
                  country_id
              ) QDU
          ) t1
        group by
          site_id,
          country_id
      ) t1
      LEFT JOIN (
        select
          site_id,
          country_id,
          site_name,
          country_name,
          SUM(total_data_change) total_data_change
        from
          (
            select
              *
            from
              (
                select
                  site_id,
                  country_id,
                  site_name,
                  country_name,
                  count(*) total_data_change
                from
                  (
                    select
                      EUV.id,
                      EUV.site_id,
                      PSC.site_name,
                      EU.participant_id,
                      EU.study_id,
                      PSC.country_id,
                      PSC.country_name
                    from
                      research_response.edc_response_version EUV
                      LEFT JOIN research_response.edc_response EU ON EUV.edc_response_id = EU.id
                      LEFT JOIN research_analytics.participant_site_country PSC on EU.participant_id = PSC.participant_id
                    where
                      EUV.version > 1
                      and EUV.state = 'SUBMITTED'
                      and EU.study_id = ?
                      and EUV.created_at <= DATE_ADD(DATE_ADD(curdate(), interval 1 day), interval -7 day) 
                      ${params.siteId ? ` and EUV.site_id in (${sitesIds}) ` : ''} 
                      ${params.participantsIds ? ` and EU.participant_id in (${participantsIds}) ` : ''} 
                      ${params.countriesIds ? ` and PSC.country_id in (${countriesIds}) ` : ''} 
                        ${params.fromDate ? ` and EUV.created_at between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day)` : ''}
                  ) t1
                group by
                  site_id,
                  country_id
              ) QD
            UNION ALL
            select
              *
            from
              (
                select
                  site_id,
                  country_id,
                  site_name,
                  country_name,
                  count(*) total_data_change
                from
                  (
                    select
                      EUV.id,
                      EUV.site_id,
                      PSC.site_name,
                      EU.participant_id,
                      EU.study_id,
                      PSC.country_id,
                      PSC.country_name
                    from
                      research_response.edc_unscheduled_packet_response_version EUV
                      LEFT JOIN research_response.edc_unscheduled_packet_response EU ON EUV.edc_unscheduled_packet_response_id = EU.id
                      LEFT JOIN research_analytics.participant_site_country PSC on EU.participant_id = PSC.participant_id
                    where
                      EUV.version > 1
                      and EUV.state = 'SUBMITTED'
                      and EU.study_id = ?
                      and EUV.created_at <= DATE_ADD(DATE_ADD(curdate(), interval 1 day), interval -7 day)
                      ${params.siteId ? ` and EUV.site_id in (${sitesIds}) ` : ''}
                      ${params.participantsIds ? ` and EU.participant_id in (${participantsIds}) ` : ''}
                      ${params.countriesIds ? ` and PSC.country_id in (${countriesIds}) ` : ''}
                        ${params.fromDate ? ` and EUV.created_at between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day)` : ''}
                  ) t1 
                group by 
                  site_id,
                  country_id
              ) QDU
          ) t1
        group by
          site_id,
          country_id
      ) t2 ON t1.site_id = t2.site_id
      and t1.country_id = t2.country_id
    `;
      console.log(`getTotalDataChange query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getTotalDataChange:', error);
      throw error;
    }
  }

  async getTotalQueryData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      let sitesIds = null;
      let countriesIds = null;
      for (let index = 0; index < 1; index++) {
        bindingParams.push(params.studyId)
      }
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      if (params.sitesIds) {
        sitesIds = convertArrayToString(params.sitesIds);
      }
      if (params.countriesIds) {
        countriesIds = convertArrayToString(params.countriesIds);
      }
      let querySql = `
        select
          site_id,
          country_id,
          site_name as siteName,
          country_name as countryName,
          
          count( 
            case when (t2.created_at <= DATE_ADD(curdate(), interval 1 day) ) 
            ${params.fromDate ? ` and (t2.created_at between '${params.fromDate}' and '${params.toDate}')` : ''} 
            then 1 end 
          ) as 'total_query',
          
          count( 
            case when (t2.created_at <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
            ${params.fromDate ? `and (t2.created_at between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day)) ` : ''}
            then 1 end 
          ) as 'total_query_prev1wk'
          from (
          select
            t1.query_id,
            PSC.study_name,
            PSC.site_id,
            PSC.site_name,
            PSC.country_id,
            PSC.country_name,
            t1.participant_id,
            t1.created_at
          from
            (
            select
              EQ.query_id,
              EQ.created_at,
              EQ.status,
              ER.id as response_id ,
              ER.participant_id
            from
              research_response.edc_response ER
              inner join (
                select
                  id as query_id,
                  created_at,
                  status,
                  response_id,
                  study_id
                from
                  research_response.edc_query
                where
                  study_id = ?) EQ on ER.id = EQ.response_id) t1
              left join research_analytics.participant_site_country PSC on t1.participant_id = PSC.participant_id 
              ${params.siteIds || params.participantsIds || countriesIds ? ' where ' : ''}
              ${params.siteIds ? ` PSC.site_id in (${siteIds}) ` : ''}
              ${params.siteIds && params.participantsIds ? ' and ' : ''}
              ${params.participantsIds ? ` PSC.participant_id in (${participantsIds}) ` : ''}
              ${(params.participantsIds && params.countriesIds) || (params.siteIds && params.countriesIds) ? ' and ' : ''}
              ${params.countriesIds ? ` PSC.country_id in (${countriesIds}) ` : ''}) t2
        group by
          site_id
      `;
      console.log(`getTotalQueryData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getTotalQueryData:', error);
      throw error;
    }
  }

  async getCompletionData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      for (let index = 0; index < 6; index++) {
        bindingParams.push(params.studyId)
      }
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      let querySql = `
      select 
      study_id, country_id, country_name as countryName, site_id, site_name as siteName, activity_name as activityName,
      SUM(n_total_completed) nTotalCompleted,
      SUM(n_total_missing) nTotalMissing,
      SUM(n_total_completed_prev1wk) nTotalCompletedPrev1Week,
      SUM(n_total_missing_prev1wk) nTotalMissingPrev1Week
      from
      (
      select * from
      (select
      study_id, country_id, country_name, site_id, site_name, activity_name,
      count(
        case when (status = 'Complete') 
        and (task_date <= DATE_ADD(curdate(), interval 1 day) ) 
        ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''} 
        then 1 end 
      ) as 'n_total_completed',
      count(
        case when (status = 'Missed') 
        and (task_date <= DATE_ADD(curdate(), interval 1 day) ) 
        ${params.fromDate ?  ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''} 
        then 1 end
      ) as 'n_total_missing',  
      count(
        case when (status = 'Complete') 
        and (task_date <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
        ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
        then 1 end 
      ) as 'n_total_completed_prev1wk',
      count(
        case when (status = 'Missed') 
        and (task_date <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
        ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''} 
        then 1 end 
      ) as 'n_total_missing_prev1wk'
      from
      (select 
      PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
      PTS.task_title as activity_name,
      COALESCE(PTS.completion_date, DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) as task_date,
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
      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
      ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}  
      ) PSC
      INNER JOIN (
      select
      pts.participant_id, PTS.study_id,
      pts.task_title, 
      pts.created_date, pts.start_day, pts.end_day, ar.id ar_id,
      'activity' as response_type, '' as visit_status, ar.end_time as completion_date
      FROM 
      (select * from research_response.participant_task_schedule where study_id = ?
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        )  pts 
      left join 
      research_response.activity_response ar on  pts.participant_id = ar.participant_id and pts.task_instance_id = ar.task_instance_id and pts.study_version_id = ar.study_version_id 
      where pts.task_type IN ('activity')
      ) PTS ON PSC.participant_id = PTS.participant_id and
                                                                    PSC.study_id = PTS.study_id
      ${params.fromDate ? `WHERE COALESCE(PTS.completion_date, DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
      ) t1
      group by activity_name,site_id) T1
      UNION
      select * from
      (select
      study_id, country_id, country_name, site_id, site_name, activity_name,
      count(
        case when (status = 'Complete') 
        and (task_date <= DATE_ADD(curdate(), interval 1 day) ) 
        ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''} 
        then 1 end 
      ) as 'n_total_completed',
      count(
        case when (status = 'Missed') 
        and (task_date <= DATE_ADD(curdate(), interval 1 day) ) 
        ${params.fromDate ?  ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''} 
        then 1 end
      ) as 'n_total_missing',  
      count(
        case when (status = 'Complete') 
        and (task_date <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
        ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
        then 1 end 
      ) as 'n_total_completed_prev1wk',
      count(
        case when (status = 'Missed') 
        and (task_date <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
        ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''} 
        then 1 end 
      ) as 'n_total_missing_prev1wk'
      from
      (select 
      PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
      PTS.task_title as activity_name,
      COALESCE(PTS.completion_date, DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) as task_date,
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
      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
      ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
      ) PSC
      INNER JOIN (
      SELECT  
      pts.participant_id, PTS.study_id,
      pts.task_title, 
      pts.created_date, pts.start_day, pts.end_day, srt.id ar_id,
      'survey' as response_type , '' as visit_status, srt.completion_time_utc as completion_date
      FROM 
      (select * from research_response.participant_task_schedule where study_id = ?
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) pts
      left join
      research_response.survey_response_tracker srt on  pts.participant_id = srt.participant_id and pts.task_instance_id = srt.task_instance_id  and pts.study_version_id = srt.study_version_id 
      where pts.task_type IN ('survey', 'epro') 
      ) PTS ON PSC.participant_id = PTS.participant_id and
                                                                    PSC.study_id = PTS.study_id
      ${params.fromDate ? `WHERE COALESCE(PTS.completion_date, DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
      ) t1
      group by activity_name,site_id) T2
      UNION 
      select * from
      (select
      study_id, country_id, country_name, site_id, site_name, activity_name,
      count(
        case when (status = 'Complete') 
        and (task_date <= DATE_ADD(curdate(), interval 1 day) ) 
        ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''} 
        then 1 end 
      ) as 'n_total_completed',
      count(
        case when (status = 'Missed') 
        and (task_date <= DATE_ADD(curdate(), interval 1 day) ) 
        ${params.fromDate ?  ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''} 
        then 1 end
      ) as 'n_total_missing',  
      count(
        case when (status = 'Complete') 
        and (task_date <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
        ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
        then 1 end 
      ) as 'n_total_completed_prev1wk',
      count(
        case when (status = 'Missed') 
        and (task_date <= DATE_ADD( DATE_ADD(curdate(), interval 1 day), interval -7 day) ) 
        ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''} 
        then 1 end 
      ) as 'n_total_missing_prev1wk'
      from
      (select 
      PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
      PTS.task_title as activity_name,
      COALESCE(PTS.completion_date, DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) as task_date,
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
      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
      ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
      ) PSC
      INNER JOIN (
      select
      pts.participant_id, PTS.study_id,
      pts.task_title, 
      pts.created_date, pts.start_day, pts.end_day, ppa.id ar_id,
      'visits' as response_type, ppa.status as visit_status, ppa.end_time as completion_date
      FROM
      (select * from research_response.participant_task_schedule where study_id = ?
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) pts
      left join research_response.pi_participant_appointment ppa on  pts.participant_id = ppa.participant_id and pts.task_instance_id = ppa.task_instanceuuid and pts.study_version_id = ppa.study_version_id 
      and  (ppa.visit_id IS NOT NULL) 
      WHERE pts.task_type IN ('telehealth')
      ) PTS ON PSC.participant_id = PTS.participant_id and
                                                                    PSC.study_id = PTS.study_id
      ${params.fromDate ? `WHERE COALESCE(PTS.completion_date, DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY)) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
      ) t1
      group by activity_name,site_id) T3) F1
      group by activity_name,site_id
      -- having nTotalCompleted > 0 or nTotalMissing > 0
      `
      console.log(`getCompletionData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getCompletionData:', error);
      throw error;
    }
  }

  async getFormStatusData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      for (let index = 0; index < 6; index++) {
        bindingParams.push(params.studyId)
      }
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      let querySql = `
      select
        d1.site_name siteName,d1.country_name countryName,d1.Bucket_type bucketType, IFNULL(d2.n_entry,0) totalEntry, 
        IFNULL(d2.n_approved,0) totalApproved,
        IFNULL(d2.n_verified,0) totalVerified, 
        IFNULL(d2.n_signed,0) totalSigned
        from (
        select 
        *
        from (select distinct(site_name) site_name,country_name from (
        select
        QR.response_id, QR.form_id, QR.study_id, QR.participant_id, 
        QF.activity_name,PSC.enrollment_date,
        QF.created_at as form_created_at,QR.data_entry_at,QFE.approve_date,QFE.verify_date,QFE.signed_at,
        PSC.country_id,PSC.country_name,PSC.site_id,PSC.site_name,PSC.study_name
        from
        (
        select ER.id as response_id, ER.form_id, ER.study_id, ER.participant_id, ER.submitted_at data_entry_at
        from 
        research_response.edc_response ER
        INNER JOIN
        (select form_id,participant_id, min(submitted_at) submitted_at
        from 
        research_response.edc_response
        where study_id = ?
        group by form_id,participant_id) AGG1 ON ER.form_id = AGG1.form_id and
                                                ER.participant_id = AGG1.participant_id and
                                                ER.submitted_at = AGG1.submitted_at
        where study_id = ?
        ) QR
        LEFT JOIN (
        select 
        t1.response_id, 
        case
        when t4.not_approved_at > t3.approved_at then null 
        else t3.approved_at
        end as approve_date,
        case when t6.not_verified_at > t5.verified_at then null
        else t5.verified_at
        end as verify_date,
        t7.signed_at
        from 
        (select distinct(response_id) from research_response.edc_response_event) t1
        LEFT JOIN (
        select response_id, max(created_at) as approved_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'APPROVED') P
        group by response_id
        ) t3 ON t1.response_id = t3.response_id
        LEFT JOIN (
        select response_id, max(created_at) as not_approved_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'NOT_APPROVED') P
        group by response_id
        ) t4 ON t1.response_id = t4.response_id
        LEFT JOIN (
        select response_id, max(created_at) as verified_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'VERIFIED') P
        group by response_id
        ) t5 ON t1.response_id = t5.response_id
        LEFT JOIN (
        select response_id, max(created_at) as not_verified_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'NOT_VERIFIED') P
        group by response_id
        ) t6 ON t1.response_id = t6.response_id
        LEFT JOIN (
        select response_id, max(created_at) as signed_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'SIGNEDOFF') P
        group by response_id
        ) t7 ON t1.response_id = t7.response_id
        ) QFE ON QR.response_id = QFE.response_id
        LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id and
                                                                    QR.study_id = PSC.study_id
        LEFT JOIN (
        select 
        id,study_id,created_at, name activity_name
        from
        research_response.edc_form
        where study_id = ?
        ) QF ON QR.form_id = QF.id and 
                                      QR.study_id = QF.study_id
        ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
        ${params.siteId ? `PSC.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
        ${params.fromDate ? `(QR.data_entry_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
        ${params.participantsIds ? `QR.participant_id in (${participantsIds}) ` : ''} 
        ) t1) t3, (
        select '0-7 days' Bucket_type union select '>7-30 days'
        union select '>30-60 days' union select '>60 days'
        ) t1
        ) d1
        LEFT JOIN
        (select Bucket_type,
        count(case when is_entry = 'entry' then 1 end) as n_entry,
        count(case when is_approved = 'approved' then 1 end) as n_approved,
        count(case when is_verified = 'verified' then 1 end) as n_verified,
        count(case when is_signed = 'signed' then 1 end) as n_signed,
        site_name
        from
        (select 
        *,
        CASE
            WHEN TIMESTAMPDIFF (day, form_created_at, NOW()) < 8 THEN '0-7 days'
            WHEN TIMESTAMPDIFF (day, form_created_at, NOW()) > 7 AND TIMESTAMPDIFF (day, form_created_at, NOW()) < 31 THEN '>7-30 days'
            WHEN TIMESTAMPDIFF (day, form_created_at, NOW()) > 30 AND TIMESTAMPDIFF (day, form_created_at, NOW()) < 61 THEN '>30-60 days'
            WHEN TIMESTAMPDIFF (day, form_created_at, NOW()) > 60 THEN '>60 days'
            END AS Bucket_type,
        CASE 
        when DAYNAME(data_entry_at) is not null then 'entry' else null end as is_entry,
        case when DAYNAME(approve_date) is not null then 'approved' else null end as is_approved,
        case when DAYNAME(verify_date) is not null then 'verified' else null end as is_verified,
        case when DAYNAME(signed_at) is not null then 'signed' else null end as is_signed

        from
        (
        select
        QR.response_id, QR.form_id, QR.study_id, QR.participant_id, 
        QF.activity_name,PSC.enrollment_date,
        QF.created_at as form_created_at,QR.data_entry_at,QFE.approve_date,QFE.verify_date,QFE.signed_at,
        PSC.country_id,PSC.country_name,PSC.site_id,PSC.site_name,PSC.study_name
        from
        (
        select ER.id as response_id, ER.form_id, ER.study_id, ER.participant_id, ER.submitted_at data_entry_at
        from 
        research_response.edc_response ER
        INNER JOIN
        (select form_id,participant_id, min(submitted_at) submitted_at
        from 
        research_response.edc_response
        where study_id = ?
        group by form_id,participant_id) AGG1 ON ER.form_id = AGG1.form_id and
                                                ER.participant_id = AGG1.participant_id and
                                                ER.submitted_at = AGG1.submitted_at
        where study_id = ?
        ) QR
        LEFT JOIN (
        select 
        t1.response_id, 
        case
        when t4.not_approved_at > t3.approved_at then null 
        else t3.approved_at
        end as approve_date,
        case when t6.not_verified_at > t5.verified_at then null
        else t5.verified_at
        end as verify_date,
        t7.signed_at
        from 
        (select distinct(response_id) from research_response.edc_response_event) t1
        LEFT JOIN (
        select response_id, max(created_at) as approved_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'APPROVED') P
        group by response_id
        ) t3 ON t1.response_id = t3.response_id
        LEFT JOIN (
        select response_id, max(created_at) as not_approved_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'NOT_APPROVED') P
        group by response_id
        ) t4 ON t1.response_id = t4.response_id
        LEFT JOIN (
        select response_id, max(created_at) as verified_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'VERIFIED') P
        group by response_id
        ) t5 ON t1.response_id = t5.response_id
        LEFT JOIN (
        select response_id, max(created_at) as not_verified_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'NOT_VERIFIED') P
        group by response_id
        ) t6 ON t1.response_id = t6.response_id
        LEFT JOIN (
        select response_id, max(created_at) as signed_at
        from 
        (select
        *
        from research_response.edc_response_event
        where event_type = 'SIGNEDOFF') P
        group by response_id
        ) t7 ON t1.response_id = t7.response_id
        ) QFE ON QR.response_id = QFE.response_id
        LEFT JOIN research_analytics.participant_site_country PSC on QR.participant_id = PSC.participant_id and
                                                                    QR.study_id = PSC.study_id
        LEFT JOIN (
        select 
        id,study_id,created_at, name activity_name
        from
        research_response.edc_form
        where study_id = ?
        ) QF ON QR.form_id = QF.id and QR.study_id = QF.study_id
        ${params.siteId || params.fromDate || params.participantsIds ? 'WHERE ' : ''}
        ${params.siteId ? `PSC.site_id = '${params.siteId}' ` : ''} ${(params.siteId && params.fromDate) || (params.siteId && params.participantsIds) ? 'AND ' : ''}
        ${params.fromDate ? `(QR.data_entry_at BETWEEN '${params.fromDate}' AND '${params.toDate}')` : ''} ${params.fromDate && params.participantsIds ? 'AND ' : ''}
        ${params.participantsIds ? `QR.participant_id in (${participantsIds}) ` : ''} 
        ) t1) t1
        group by Bucket_type,site_name) d2 ON d1.Bucket_type = d2.Bucket_type and d1.site_name = d2.site_name
      `
      console.log(`getFormStatusData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getFormStatusData:', error);
      throw error;
    }
  }

  async getQueriesTimeData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      bindingParams.push(params.studyId)
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      let querySql = `
      select 
      t1.site_id, 
      t1.study_id, 
      t1.study_name, 
      t1.country_name as countryName, 
      t1.site_name as siteName, 
      t1.avg_queries_closed as avgHourQueriesClose, 
      t2.activity_name as longestActivity 
    from 
      (
        SELECT 
          QL.site_id as site_id, 
          QL.study_id as study_id, 
          QL.study_name as study_name, 
          QL.country_name as country_name, 
          QL.site_name as site_name, 
          ROUND(
            AVG(Q4.different_hour), 
            1
          ) as avg_queries_closed, 
          MAX(Q4.different_hour) as longest_closed 
        FROM 
          (
            SELECT 
              Q3.participant_id as participant_id, 
              Q3.study_id as study_id, 
              Q3.activity_name as activity_name, 
              Q3.different_hour, 
              Q3.new_status as new_status 
            FROM 
              (
                SELECT 
                  EQ.id as query_id, 
                  EQ.status as status, 
                  EQ.response_id as response_id, 
                  EQ.study_id as study_id, 
                  QF.activity_name as activity_name, 
                  Q2.response_event_id as response_event_id, 
                  Q2.participant_id as participant_id, 
                  EQ.created_at as min_created_at, 
                  Q2.max_created_at as max_created_at, 
                  CASE WHEN Q2.response_event_id is null 
                  AND EQ.status = 'OPEN' THEN 'OPEN' WHEN Q2.response_event_id is not null 
                  AND EQ.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN EQ.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
                  TIMESTAMPDIFF (
                    hour, EQ.created_at, Q2.max_created_at
                  ) AS 'different_hour' 
                FROM 
                  research_response.edc_query EQ 
                  LEFT JOIN (
                    SELECT 
                      ER.participant_id as participant_id, 
                      ER.id as response_id, 
                      ER.edc_unsch_pack_id as form_id, 
                      ERE.id as response_event_id, 
                      ERE.max_created_at as max_created_at 
                    FROM 
                      research_response.edc_unscheduled_packet_response ER 
                      LEFT JOIN (
                        SELECT 
                          response_id, 
                          id, 
                          MIN(created_at) as min_created_at, 
                          MAX(created_at) as max_created_at 
                        FROM 
                          research_response.edc_response_event 
                        GROUP BY 
                          response_id
                      ) ERE on ER.id = ERE.response_id
                  ) Q2 ON EQ.response_id = Q2.response_id 
                  LEFT JOIN (
                    SELECT 
                      id, 
                      study_id, 
                      packet_name as activity_name 
                    from 
                      research_response.edc_unscheduled_packet
                  ) QF ON Q2.form_id = QF.id 
                WHERE 
                  1 = CASE WHEN EQ.status = 'CLOSE' THEN 1 ELSE NULL END ${params.fromDate ? ` AND EQ.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}'` : '' }
              ) Q3 
            union all 
            SELECT 
              Q3.participant_id as participant_id, 
              Q3.study_id as study_id, 
              Q3.activity_name as activity_name, 
              Q3.different_hour, 
              Q3.new_status as new_status 
            FROM 
              (
                SELECT 
                  EQ.id as query_id, 
                  EQ.status as status, 
                  EQ.response_id as response_id, 
                  EQ.study_id as study_id, 
                  QF.activity_name as activity_name, 
                  Q2.response_event_id as response_event_id, 
                  Q2.participant_id as participant_id, 
                  EQ.created_at as min_created_at, 
                  Q2.max_created_at as max_created_at, 
                  CASE WHEN Q2.response_event_id is null 
                  AND EQ.status = 'OPEN' THEN 'OPEN' WHEN Q2.response_event_id is not null 
                  AND EQ.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN EQ.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
                  TIMESTAMPDIFF (
                    hour, EQ.created_at, Q2.max_created_at
                  ) AS 'different_hour' 
                FROM 
                  research_response.edc_query EQ 
                  LEFT JOIN (
                    SELECT 
                      ER.participant_id as participant_id, 
                      ER.id as response_id, 
                      ER.form_id, 
                      ERE.id as response_event_id, 
                      ERE.max_created_at as max_created_at 
                    FROM 
                      research_response.edc_response ER 
                      LEFT JOIN (
                        SELECT 
                          response_id, 
                          id, 
                          MIN(created_at) as min_created_at, 
                          MAX(created_at) as max_created_at 
                        FROM 
                          research_response.edc_response_event 
                        GROUP BY 
                          response_id
                      ) ERE on ER.id = ERE.response_id
                  ) Q2 ON EQ.response_id = Q2.response_id 
                  LEFT JOIN (
                    SELECT 
                      id, 
                      study_id, 
                      name activity_name 
                    from 
                      research_response.edc_form
                  ) QF ON Q2.form_id = QF.id 
                WHERE 
                  1 = CASE WHEN EQ.status = 'CLOSE' THEN 1 ELSE NULL END ${params.fromDate ? ` AND EQ.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}'` : '' }
              ) Q3
          ) Q4 
          LEFT JOIN research_analytics.participant_site_country QL ON Q4.participant_id = QL.participant_id 
          AND QL.study_id = Q4.study_id 
        WHERE 
          QL.study_id = ? ${params.siteId ? ` and QL.site_id = '${params.siteId}' ` : '' } ${params.participantsIds ? `and Q4.participant_id in (${participantsIds}) ` : '' } 
        GROUP BY 
          study_id
      ) t1 
      LEFT JOIN (
        SELECT 
          EQ.id as query_id, 
          EQ.status as status, 
          EQ.response_id as response_id, 
          EQ.study_id as study_id, 
          QF.activity_name as activity_name, 
          Q2.response_event_id as response_event_id, 
          Q2.participant_id as participant_id, 
          EQ.created_at as min_created_at, 
          Q2.max_created_at as max_created_at, 
          CASE WHEN Q2.response_event_id is null 
          AND EQ.status = 'OPEN' THEN 'OPEN' WHEN Q2.response_event_id is not null 
          AND EQ.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN EQ.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
          TIMESTAMPDIFF (
            hour, EQ.created_at, Q2.max_created_at
          ) AS 'different_hour' 
        FROM 
          research_response.edc_query EQ 
          LEFT JOIN (
            SELECT 
              ER.participant_id as participant_id, 
              ER.id as response_id, 
              ER.edc_unsch_pack_id as form_id, 
              ERE.id as response_event_id, 
              ERE.max_created_at as max_created_at 
            FROM 
              research_response.edc_unscheduled_packet_response ER 
              LEFT JOIN (
                SELECT 
                  response_id, 
                  id, 
                  MIN(created_at) as min_created_at, 
                  MAX(created_at) as max_created_at 
                FROM 
                  research_response.edc_response_event 
                GROUP BY 
                  response_id
              ) ERE on ER.id = ERE.response_id
          ) Q2 ON EQ.response_id = Q2.response_id 
          LEFT JOIN (
            SELECT 
              id, 
              study_id, 
              packet_name as activity_name 
            from 
              research_response.edc_unscheduled_packet
          ) QF ON Q2.form_id = QF.id 
        WHERE 
          1 = CASE WHEN EQ.status = 'CLOSE' THEN 1 ELSE NULL END ${params.fromDate ? ` AND EQ.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}'` : '' } 
        union all 
        SELECT 
          EQ.id as query_id, 
          EQ.status as status, 
          EQ.response_id as response_id, 
          EQ.study_id as study_id, 
          QF.activity_name as activity_name, 
          Q2.response_event_id as response_event_id, 
          Q2.participant_id as participant_id, 
          EQ.created_at as min_created_at, 
          Q2.max_created_at as max_created_at, 
          CASE WHEN Q2.response_event_id is null 
          AND EQ.status = 'OPEN' THEN 'OPEN' WHEN Q2.response_event_id is not null 
          AND EQ.status = 'OPEN' THEN 'RESPONDED NOT CLOSED' WHEN EQ.status = 'CLOSE' THEN 'CLOSE' END AS new_status, 
          TIMESTAMPDIFF (
            hour, EQ.created_at, Q2.max_created_at
          ) AS 'different_hour' 
        FROM 
          research_response.edc_query EQ 
          LEFT JOIN (
            SELECT 
              ER.participant_id as participant_id, 
              ER.id as response_id, 
              ER.form_id, 
              ERE.id as response_event_id, 
              ERE.max_created_at as max_created_at 
            FROM 
              research_response.edc_response ER 
              LEFT JOIN (
                SELECT 
                  response_id, 
                  id, 
                  MIN(created_at) as min_created_at, 
                  MAX(created_at) as max_created_at 
                FROM 
                  research_response.edc_response_event 
                GROUP BY 
                  response_id
              ) ERE on ER.id = ERE.response_id
          ) Q2 ON EQ.response_id = Q2.response_id 
          LEFT JOIN (
            SELECT 
              id, 
              study_id, 
              name activity_name 
            from 
              research_response.edc_form
          ) QF ON Q2.form_id = QF.id 
        WHERE 
          1 = CASE WHEN EQ.status = 'CLOSE' THEN 1 ELSE NULL END ${params.fromDate ? ` AND EQ.created_at BETWEEN '${params.fromDate}' AND '${params.toDate}'` : '' }
      ) t2 ON t1.longest_closed = t2.different_hour
      `
      console.log(`getQueriesTimeData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getQueriesTimeData:', error);
      throw error;
    }
  }

  async getParticipantCompletionData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      for (let index = 0; index < 6; index++) {
        bindingParams.push(params.studyId)
      }
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      let querySql = `
        select 
        study_id, country_id, country_name as countryName, site_id, site_name as siteName, participant_id as participantId,
        SUM(n_total_completed) totalCompleted,
        SUM(n_total_missing) totalMissing
        from
        (
        select * from
        (select
        study_id, country_id, country_name, site_id, site_name, participant_id,
        count(case when status = 'Complete' then 1 end) n_total_completed,
        count(case when status = 'Missed' then 1 end) n_total_missing
        from
        (select 
        PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
        PTS.task_title as activity_name,
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
        ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) PSC
        INNER JOIN (
        select
        pts.participant_id, PTS.study_id,
        pts.task_title, 
        pts.created_date, pts.start_day, pts.end_day, ar.id ar_id,
        'activity' as response_type, '' as visit_status
        FROM 
        (select * from research_response.participant_task_schedule where study_id = ?
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
          )  pts 
        left join 
        research_response.activity_response ar on  pts.participant_id = ar.participant_id and pts.task_instance_id = ar.task_instance_id and pts.study_version_id = ar.study_version_id 
        where pts.task_type IN ('activity')
        ) PTS ON PSC.participant_id = PTS.participant_id and PSC.study_id = PTS.study_id
        ${params.fromDate ? `WHERE DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
        ) t1
        group by participant_id) T1
        UNION
        select * from
        (select
        study_id, country_id, country_name, site_id, site_name, participant_id,
        count(case when status = 'Complete' then 1 end) n_total_completed,
        count(case when status = 'Missed' then 1 end) n_total_missing
        from
        (select 
        PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
        PTS.task_title as activity_name,
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
        ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) PSC
        INNER JOIN (
        SELECT  
        pts.participant_id, PTS.study_id,
        pts.task_title, 
        pts.created_date, pts.start_day, pts.end_day, srt.id ar_id,
        'survey' as response_type , '' as visit_status
        FROM 
        (select * from research_response.participant_task_schedule where study_id = ?
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
          ) pts
        left join
        research_response.survey_response_tracker srt on  pts.participant_id = srt.participant_id and pts.task_instance_id = srt.task_instance_id  and pts.study_version_id = srt.study_version_id 
        where pts.task_type IN ('survey', 'epro') 
        ) PTS ON PSC.participant_id = PTS.participant_id and PSC.study_id = PTS.study_id
        ${params.fromDate ? `WHERE DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
        ) t1
        group by participant_id) T2
        UNION 
        select * from
        (select
        study_id, country_id, country_name, site_id, site_name, participant_id,
        count(case when status = 'Complete' then 1 end) n_total_completed,
        count(case when status = 'Missed' then 1 end) n_total_missing
        from
        (select 
        PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
        PTS.task_title as activity_name,
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
        ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) PSC
        INNER JOIN (
        select
        pts.participant_id, PTS.study_id,
        pts.task_title, 
        pts.created_date, pts.start_day, pts.end_day, ppa.id ar_id,
        'visits' as response_type, ppa.status as visit_status
        FROM
        (select * from research_response.participant_task_schedule where study_id = ?
          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
          ) pts
        left join research_response.pi_participant_appointment ppa on  pts.participant_id = ppa.participant_id and pts.task_instance_id = ppa.task_instanceuuid and pts.study_version_id = ppa.study_version_id 
        and  (ppa.visit_id IS NOT NULL) 
        WHERE pts.task_type IN ('telehealth')
        ) PTS ON PSC.participant_id = PTS.participant_id and PSC.study_id = PTS.study_id
        ${params.fromDate ? `WHERE DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
        ) t1
        group by participant_id) T3) F1
        group by country_id, site_id, participant_id
      `
      console.log(`getParticipantCompletionData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getParticipantCompletionData:', error);
      throw error;
    }
  }

  async getMissingData(params){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      let participantsIds = null;
      for (let index = 0; index < 6; index++) {
        bindingParams.push(params.studyId)
      }
      if (params.participantsIds) {
        participantsIds = convertArrayToString(params.participantsIds);
      }
      let querySql = `
      SELECT
      study_id, country_id, country_name as countryName, site_id, site_name as siteName,
      SUM(n_missed_7_days) n_missed_7_days,
      SUM(n_missed_7_30_days) n_missed_7_30_days,
      SUM(n_missed_30_60_days) n_missed_30_60_days,
      SUM(n_missed_60_days) n_missed_60_days
      FROM
      (
      SELECT *
      FROM
      (select
      study_id, country_id, country_name, site_id, site_name,
      count(case when status = 'Missed' and Bucket_type = '0-7 days' then 1 end) 'n_missed_7_days',
      count(case when status = 'Missed' and Bucket_type = '7-30 days' then 1 end) 'n_missed_7_30_days',
      count(case when status = 'Missed' and Bucket_type = '30-60 days' then 1 end) 'n_missed_30_60_days',
      count(case when status = 'Missed' and Bucket_type = '60+ days' then 1 end) 'n_missed_60_days'
      from
      (select
      PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
      PTS.task_title as activity_name,
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
      END AS status,
      CASE
          WHEN TIMESTAMPDIFF (day, PSC.participant_start_date, NOW()) < 8 THEN '0-7 days'
          WHEN TIMESTAMPDIFF (day, PSC.participant_start_date, NOW()) > 7 AND TIMESTAMPDIFF (day, PSC.participant_start_date, NOW()) < 31 THEN '7-30 days'
          WHEN TIMESTAMPDIFF (day, PSC.participant_start_date, NOW()) > 30 AND TIMESTAMPDIFF (day, PSC.participant_start_date, NOW()) < 61 THEN '30-60 days'
          WHEN TIMESTAMPDIFF (day, PSC.participant_start_date, NOW()) > 60 THEN '60+ days'
          END AS Bucket_type
      from
      (
      select *
      from
      research_analytics.participant_site_country
      where study_id = ? 
            ${params.siteId ? ` and site_id = '${params.siteId}' ` : ''}
            ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
      ) PSC
      INNER JOIN (
      select
      pts.participant_id, PTS.study_id,
       pts.task_title,
      pts.created_date, pts.start_day, pts.end_day, ar.id ar_id,
       'activity' as response_type, '' as visit_status
      FROM
      (select * from research_response.participant_task_schedule where study_id = ?
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        )  pts
      left join
      research_response.activity_response ar on  pts.participant_id = ar.participant_id and pts.task_instance_id = ar.task_instance_id and pts.study_version_id = ar.study_version_id
      where pts.task_type IN ('activity')
      ) PTS ON PSC.participant_id = PTS.participant_id and PSC.study_id = PTS.study_id
      ${params.fromDate ? `WHERE DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''} 
      ) t1
      group by site_name) T1
      UNION
      SELECT *
      FROM
      (select
      study_id, country_id, country_name, site_id, site_name,
      count(case when status = 'Missed' and Bucket_type = '0-7 days' then 1 end) 'n_missed_7_days',
      count(case when status = 'Missed' and Bucket_type = '7-30 days' then 1 end) 'n_missed_7_30_days',
      count(case when status = 'Missed' and Bucket_type = '30-60 days' then 1 end) 'n_missed_30_60_days',
      count(case when status = 'Missed' and Bucket_type = '60+ days' then 1 end) 'n_missed_60_days'
      from
      (select
      PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
      PTS.task_title as activity_name, PTS.end_day, PSC.participant_start_date,
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
      END AS status,
      CASE
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) < 8 THEN '0-7 days'
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) > 7 AND TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) < 31 THEN '7-30 days'
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) > 30 AND TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) < 61 THEN '30-60 days'
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) > 60 THEN '60+ days'
          END AS Bucket_type
      from
      (
      select *
      from
      research_analytics.participant_site_country
      where study_id = ? 
            ${params.siteId ? ` and site_id = '${params.siteId}' ` : ''}
            ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
      ) PSC
      INNER JOIN (
      SELECT
      pts.participant_id, PTS.study_id,
       pts.task_title,
      pts.created_date, pts.start_day, pts.end_day, srt.id ar_id,
      'survey' as response_type , '' as visit_status
      FROM
      (select * from research_response.participant_task_schedule where study_id = ?
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) pts
      left join
      research_response.survey_response_tracker srt on  pts.participant_id = srt.participant_id and pts.task_instance_id = srt.task_instance_id  and pts.study_version_id = srt.study_version_id
      where pts.task_type IN ('survey', 'epro')
      ) PTS ON PSC.participant_id = PTS.participant_id and PSC.study_id = PTS.study_id
      ${params.fromDate ? `WHERE DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''} 
      ) t1
      group by site_name) T2
      UNION
      select * from
      (select
      study_id, country_id, country_name, site_id, site_name,
      count(case when status = 'Missed' and Bucket_type = '0-7 days' then 1 end) 'n_missed_7_days',
      count(case when status = 'Missed' and Bucket_type = '7-30 days' then 1 end) 'n_missed_7_30_days',
      count(case when status = 'Missed' and Bucket_type = '30-60 days' then 1 end) 'n_missed_30_60_days',
      count(case when status = 'Missed' and Bucket_type = '60+ days' then 1 end) 'n_missed_60_days'
      from
      (select
      PSC.participant_id, PSC.site_name, PSC.site_id, PSC.study_name, PSC.study_id, PSC.country_id, PSC.country_name,
      PTS.task_title as activity_name, PTS.end_day, PSC.participant_start_date,
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
      END AS status,
      CASE
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) < 8 THEN '0-7 days'
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) > 7 AND TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) < 31 THEN '7-30 days'
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) > 30 AND TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) < 61 THEN '30-60 days'
          WHEN TIMESTAMPDIFF(day, PSC.participant_start_date, NOW()) > 60 THEN '60+ days'
          END AS Bucket_type
      from
      (
      select *
      from
      research_analytics.participant_site_country
      where study_id = ? 
            ${params.siteId ? ` and site_id = '${params.siteId}' ` : ''}
            ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
      ) PSC
      INNER JOIN (
      select
      pts.participant_id, PTS.study_id,
       pts.task_title,
      pts.created_date, pts.start_day, pts.end_day, ppa.id ar_id,
       'visits' as response_type, ppa.status as visit_status
      FROM
      (select * from research_response.participant_task_schedule where study_id = ?
        ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''} 
        ) pts
      left join research_response.pi_participant_appointment ppa on  pts.participant_id = ppa.participant_id and pts.task_instance_id = ppa.task_instanceuuid and pts.study_version_id = ppa.study_version_id
      and  (ppa.visit_id IS NOT NULL)
      WHERE pts.task_type IN ('telehealth')
      ) PTS ON PSC.participant_id = PTS.participant_id and PSC.study_id = PTS.study_id
      ${params.fromDate ? `WHERE DATE_ADD(PSC.participant_start_date, INTERVAL PTS.end_day DAY) BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''} 
      ) t1
      group by site_name) T3
      ) q34
      GROUP BY site_name
      `
      console.log(`getMissingData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getMissingData:', error);
      throw error;
    }
}
}

module.exports = EnrollmentModel;
