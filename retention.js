const BaseModel = require("./baseModel");
const {constants: {DATABASE: {RESEARCH_ANALYTICS_DB}}} = require('../constants')
const {convertArrayToString} = require('../utils')
const utils = require('../config/utils');

/**
 * Class representing a message model.
 * @class
 */
class RetentionModel extends BaseModel {
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

  async getMilestoneData(params) {
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      for (let index = 0; index < 1; index++) {
        bindingParams.push(params.studyId)
      }

      const querySql = `
        select
          study_id
        from
          research.milestones
        where study_id = ?
      `;

      console.log(`getMilestoneData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getMilestoneData:', error);
      throw error;
    }
  }

  async getMilestoneCohortData(params) {
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      for (let index = 0; index < 30; index++) {
        bindingParams.push(params.studyId)
      }

      const querySql = `
        select
          join_date,
          bucket_name,
          bucket_order,
          n_participant,
          participation_rate
        from
          (
            select
              QFC.date_id,
              join_date,
              bucket_name,
              bucket_order,
              n_participant,
              participation_rate
            from
              (
                select
                  QD.date_id,
                  DATE_FORMAT(QD.join_date, '%b-%Y') as join_date,
                  QD.milestone_name bucket_name,
                  QD.sort_order + 2 as bucket_order,
                  IFNULL(QMG.n_participant, 0) as n_participant,
                  case when QD.milestone_name = 'Users' then IFNULL(QPC.n_participant, 0) when IFNULL(QPC.n_participant, 0) = 0 then null when QD.milestone_name = 'DAY 0'
                  and IFNULL(QPC.n_participant, 0) > 0 then 1 WHEN QMG.participation_rate is not null then QMG.participation_rate WHEN (
                    DATE_ADD(QD.join_date, INTERVAL QJD.start_day DAY) <= NOW()
                  )
                  AND QMG.participation_rate is null then 0 WHEN DATE_ADD(QD.join_date, INTERVAL QJD.start_day DAY) > NOW() then null else null end as participation_rate
                from
                  (
                    select
                      *
                    from
                      (
                        select
                          t1.join_date,
                          (@csum := @csum + 1) as date_id
                        from
                          (
                            select
                              DISTINCT DATE(
                                CONCAT(
                                  EXTRACT(
                                    year
                                    from
                                      gen_date
                                  ),
                                  '-',
                                  EXTRACT(
                                    MONTH
                                    from
                                      gen_date
                                  ),
                                  '-01'
                                )
                              ) as join_date
                            from
                              (
                                select
                                  adddate(
                                    '1970-01-01',
                                    t4 * 10000 + t3 * 1000 + t2 * 100 + t1 * 10 + t0
                                  ) gen_date
                                from
                                  (
                                    select
                                      0 t0
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t0,
                                  (
                                    select
                                      0 t1
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t1,
                                  (
                                    select
                                      0 t2
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t2,
                                  (
                                    select
                                      0 t3
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t3,
                                  (
                                    select
                                      0 t4
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t4
                              ) v
                            where
                              gen_date between (
                                select
                                  study_start_date
                                from
                                  research.study_meta_data
                                where
                                  id = ?
                              )
                              and NOW()
                          ) t1
                          CROSS JOIN (
                            SELECT
                              @csum := 0
                          ) AS dummy
                      ) QD,
                      (
                        select
                          'xy' milestone_id,
                          'Users' as milestone_name,
                          -2 as sort_order
                        UNION
                        select
                          'x' milestone_id,
                          'DAY 0' as milestone_name,
                          -1 as sort_order
                        UNION
                          (
                            select
                              milestone_id,
                              display_name as milestone_name,
                              sort_order
                            from
                              research.milestones
                            where
                              study_id = ?
                          )
                      ) QM
                  ) QD
                  LEFT JOIN (
                    select
                      DATE(CONCAT(year, '-', month, '-01')) as join_date,
                      milestone_name,
                      milestone_id,
                      QM.sort_order,
                      count(participant_id) as n_participant,
                      SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate
                    from
                      (
                        select
                          QD.study_id,
                          QD.country_id,
                          QD.countryName,
                          QD.site_id,
                          QD.siteName,
                          QD.milestone_id,
                          QD.nTotalCompleted,
                          QD.nTotalMissing,
                          QD.participant_id,
                          QD.participant_start_date,
                          QD.is_participate,
                          RM.display_name as milestone_name,
                          RM.sort_order,
                          EXTRACT(
                            MONTH
                            from
                              QD.participant_start_date
                          ) AS 'month',
                          EXTRACT(
                            year
                            from
                              QD.participant_start_date
                          ) AS year
                        from
                          (
                            select
                              study_id,
                              country_id,
                              country_name as countryName,
                              site_id,
                              participant_start_date,
                              site_name as siteName,
                              milestone_id,
                              SUM(n_total_completed) nTotalCompleted,
                              SUM(n_total_missing) nTotalMissing,
                              participant_id,
                              case when SUM(n_total_completed) > 0 then 1 else 0 end as is_participate
                            from
                              (
                                select
                                  *
                                from
                                  (
                                    select
                                      study_id,
                                      country_id,
                                      country_name,
                                      site_id,
                                      participant_start_date,
                                      site_name,
                                      activity_name,
                                      participant_id,
                                      milestone_id,
                                      count(case when status = 'Complete' then 1 end) n_total_completed,
                                      count(case when status = 'Missed' then 1 end) n_total_missing
                                    from
                                      (
                                        select
                                          PSC.participant_id,
                                          PSC.site_name,
                                          PSC.site_id,
                                          participant_start_date,
                                          PSC.study_name,
                                          PSC.study_id,
                                          PSC.country_id,
                                          PSC.country_name,
                                          PTS.task_title as activity_name,
                                          PTS.milestone_id,
                                          CASE WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                          AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' END AS status
                                        from
                                          (
                                            select
                                              *
                                            from
                                              research_analytics.participant_site_country
                                            where
                                              study_id = ?
                                              ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ) PSC
                                          INNER JOIN (
                                            select
                                              pts.participant_id,
                                              PTS.study_id,
                                              pts.task_title,
                                              pts.created_date,
                                              pts.start_day,
                                              pts.end_day,
                                              ar.id ar_id,
                                              pts.milestone_id,
                                              'activity' as response_type,
                                              '' as visit_status,
                                              ar.end_time as completion_date
                                            FROM
                                              (
                                                select
                                                  *
                                                from
                                                  research_response.participant_task_schedule
                                                where
                                                  study_id = ?
                                                  and schedule_type = 'milestone'
                                              ) pts
                                              left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                              and pts.task_instance_id = ar.task_instance_id
                                              and pts.study_version_id = ar.study_version_id
                                            where
                                              pts.task_type IN ('activity')
                                          ) PTS ON PSC.participant_id = PTS.participant_id
                                          and PSC.study_id = PTS.study_id
                                        ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                      ) t1
                                    group by
                                      participant_id,
                                      milestone_id
                                  ) T1
                                UNION
                                select
                                  *
                                from
                                  (
                                    select
                                      study_id,
                                      country_id,
                                      country_name,
                                      site_id,
                                      participant_start_date,
                                      site_name,
                                      activity_name,
                                      participant_id,
                                      milestone_id,
                                      count(case when status = 'Complete' then 1 end) n_total_completed,
                                      count(case when status = 'Missed' then 1 end) n_total_missing
                                    from
                                      (
                                        select
                                          PSC.participant_id,
                                          PSC.site_name,
                                          PSC.site_id,
                                          participant_start_date,
                                          PSC.study_name,
                                          PSC.study_id,
                                          PSC.country_id,
                                          PSC.country_name,
                                          PTS.task_title as activity_name,
                                          PTS.milestone_id,
                                          CASE WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                          AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' END AS status
                                        from
                                          (
                                            select
                                              *
                                            from
                                              research_analytics.participant_site_country
                                            where
                                              study_id = ?
                                              ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ) PSC
                                          INNER JOIN (
                                            SELECT
                                              pts.participant_id,
                                              PTS.study_id,
                                              pts.task_title,
                                              pts.created_date,
                                              pts.start_day,
                                              pts.end_day,
                                              srt.id ar_id,
                                              pts.milestone_id,
                                              'survey' as response_type,
                                              '' as visit_status,
                                              srt.completion_time_utc as completion_date
                                            FROM
                                              (
                                                select
                                                  *
                                                from
                                                  research_response.participant_task_schedule
                                                where
                                                  study_id = ?
                                                  and schedule_type = 'milestone'
                                              ) pts
                                              left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                              and pts.task_instance_id = srt.task_instance_id
                                              and pts.study_version_id = srt.study_version_id
                                            where
                                              pts.task_type IN ('survey', 'epro')
                                          ) PTS ON PSC.participant_id = PTS.participant_id
                                          and PSC.study_id = PTS.study_id
                                        ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                      ) t1
                                    group by
                                      participant_id,
                                      milestone_id
                                  ) T2
                                UNION
                                select
                                  *
                                from
                                  (
                                    select
                                      study_id,
                                      country_id,
                                      country_name,
                                      site_id,
                                      participant_start_date,
                                      site_name,
                                      activity_name,
                                      participant_id,
                                      milestone_id,
                                      count(case when status = 'Complete' then 1 end) n_total_completed,
                                      count(case when status = 'Missed' then 1 end) n_total_missing
                                    from
                                      (
                                        select
                                          PSC.participant_id,
                                          PSC.site_name,
                                          PSC.site_id,
                                          participant_start_date,
                                          PSC.study_name,
                                          PSC.study_id,
                                          PSC.country_id,
                                          PSC.country_name,
                                          PTS.task_title as activity_name,
                                          PTS.milestone_id,
                                          CASE WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                          AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' END AS status
                                        from
                                          (
                                            select
                                              *
                                            from
                                              research_analytics.participant_site_country
                                            where
                                              study_id = ?
                                              ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ) PSC
                                          INNER JOIN (
                                            select
                                              pts.participant_id,
                                              PTS.study_id,
                                              pts.task_title,
                                              pts.created_date,
                                              pts.start_day,
                                              pts.end_day,
                                              ppa.id ar_id,
                                              pts.milestone_id,
                                              'visits' as response_type,
                                              ppa.status as visit_status,
                                              ppa.end_time as completion_date
                                            FROM
                                              (
                                                select
                                                  *
                                                from
                                                  research_response.participant_task_schedule
                                                where
                                                  study_id = ?
                                                  and schedule_type = 'milestone'
                                              ) pts
                                              left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                              and pts.task_instance_id = ppa.task_instanceuuid
                                              and pts.study_version_id = ppa.study_version_id
                                              and (ppa.visit_id IS NOT NULL)
                                            WHERE
                                              pts.task_type IN ('telehealth')
                                          ) PTS ON PSC.participant_id = PTS.participant_id
                                          and PSC.study_id = PTS.study_id
                                        ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                      ) t1
                                    group by
                                      participant_id,
                                      milestone_id
                                  ) T3
                              ) F1
                            where
                              participant_start_date is not null
                            group by
                              participant_id,
                              milestone_id
                          ) QD
                          left join (
                            select
                              milestone_id,
                              display_name,
                              sort_order
                            from
                              research.milestones
                          ) RM ON QD.milestone_id = RM.milestone_id
                        order by
                          RM.sort_order
                      ) QM
                    group by
                      month,
                      year,
                      sort_order
                  ) QMG ON QD.join_date = QMG.join_date
                  and QD.sort_order = QMG.sort_order
                  LEFT JOIN (
                    select
                      min(start_day) as start_day,
                      milestone_id
                    from
                      research_response.participant_task_schedule
                    where
                      study_id = ?
                    group by
                      milestone_id
                  ) QJD ON QD.milestone_id = QJD.milestone_id
                  LEFT JOIN (
                    select
                      join_date,
                      count(participant_id) n_participant
                    from
                      (
                        select
                          participant_id,
                          DATE_FORMAT(DATE(modified_time), '%b-%Y') as join_date
                        from
                          research.participant_status_history
                        where
                          new_status in ('ACTIVE')
                          and study_id = ?
                      ) t1
                    group by
                      join_date
                  ) QPC ON DATE_FORMAT(QD.join_date, '%b-%Y') = QPC.join_date
                ORDER BY
                  QD.date_id,
                  QD.sort_order
              ) QFC
            UNION
            select
              999 date_id,
              join_date,
              bucket_name,
              bucket_order,
              n_participant,
              participation_rate
            from
              (
                select
                  'All Users' join_date,
                  bucket_name,
                  999 bucket_order,
                  n_participant,
                  SUM(participation_rate) participation_rate
                from
                  (
                    select
                      QD.date_id,
                      DATE_FORMAT(QD.join_date, '%b-%Y') as join_date,
                      QD.milestone_name bucket_name,
                      QD.sort_order + 2 as bucket_order,
                      IFNULL(QMG.n_participant, 0) as n_participant,
                      case when QD.milestone_name = 'Users' then IFNULL(QPC.n_participant, 0) when IFNULL(QPC.n_participant, 0) = 0 then null when QD.milestone_name = 'DAY 0'
                      and IFNULL(QPC.n_participant, 0) > 0 then 1 WHEN QMG.participation_rate is not null then QMG.participation_rate WHEN (
                        DATE_ADD(QD.join_date, INTERVAL QJD.start_day DAY) <= NOW()
                      )
                      AND QMG.participation_rate is null then 0 WHEN DATE_ADD(QD.join_date, INTERVAL QJD.start_day DAY) > NOW() then null else null end as participation_rate
                    from
                      (
                        select
                          *
                        from
                          (
                            select
                              t1.join_date,
                              (@csum := @csum + 1) as date_id
                            from
                              (
                                select
                                  DISTINCT DATE(
                                    CONCAT(
                                      EXTRACT(
                                        year
                                        from
                                          gen_date
                                      ),
                                      '-',
                                      EXTRACT(
                                        MONTH
                                        from
                                          gen_date
                                      ),
                                      '-01'
                                    )
                                  ) as join_date
                                from
                                  (
                                    select
                                      adddate(
                                        '1970-01-01',
                                        t4 * 10000 + t3 * 1000 + t2 * 100 + t1 * 10 + t0
                                      ) gen_date
                                    from
                                      (
                                        select
                                          0 t0
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t0,
                                      (
                                        select
                                          0 t1
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t1,
                                      (
                                        select
                                          0 t2
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t2,
                                      (
                                        select
                                          0 t3
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t3,
                                      (
                                        select
                                          0 t4
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t4
                                  ) v
                                where
                                  gen_date between (
                                    select
                                      study_start_date
                                    from
                                      research.study_meta_data
                                    where
                                      id = ?
                                  )
                                  and NOW()
                              ) t1
                              CROSS JOIN (
                                SELECT
                                  @csum := 0
                              ) AS dummy
                          ) QD,
                          (
                            select
                              'xy' milestone_id,
                              'Users' as milestone_name,
                              -2 as sort_order
                            UNION
                            select
                              'x' milestone_id,
                              'DAY 0' as milestone_name,
                              -1 as sort_order
                            UNION
                              (
                                select
                                  milestone_id,
                                  display_name as milestone_name,
                                  sort_order
                                from
                                  research.milestones
                                where
                                  study_id = ?
                              )
                          ) QM
                      ) QD
                      LEFT JOIN (
                        select
                          DATE(CONCAT(year, '-', month, '-01')) as join_date,
                          milestone_name,
                          milestone_id,
                          QM.sort_order,
                          count(participant_id) as n_participant,
                          SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate
                        from
                          (
                            select
                              QD.study_id,
                              QD.country_id,
                              QD.countryName,
                              QD.site_id,
                              QD.siteName,
                              QD.milestone_id,
                              QD.nTotalCompleted,
                              QD.nTotalMissing,
                              QD.participant_id,
                              QD.participant_start_date,
                              QD.is_participate,
                              RM.display_name as milestone_name,
                              RM.sort_order,
                              EXTRACT(
                                MONTH
                                from
                                  QD.participant_start_date
                              ) AS 'month',
                              EXTRACT(
                                year
                                from
                                  QD.participant_start_date
                              ) AS year
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name as countryName,
                                  site_id,
                                  participant_start_date,
                                  site_name as siteName,
                                  milestone_id,
                                  SUM(n_total_completed) nTotalCompleted,
                                  SUM(n_total_missing) nTotalMissing,
                                  participant_id,
                                  case when SUM(n_total_completed) > 0 then 1 else 0 end as is_participate
                                from
                                  (
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          milestone_id,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              PTS.milestone_id,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ar.id ar_id,
                                                  pts.milestone_id,
                                                  'activity' as response_type,
                                                  '' as visit_status,
                                                  ar.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'milestone'
                                                  ) pts
                                                  left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                                  and pts.task_instance_id = ar.task_instance_id
                                                  and pts.study_version_id = ar.study_version_id
                                                where
                                                  pts.task_type IN ('activity')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? `WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          milestone_id
                                      ) T1
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          milestone_id,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              PTS.milestone_id,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                SELECT
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  srt.id ar_id,
                                                  pts.milestone_id,
                                                  'survey' as response_type,
                                                  '' as visit_status,
                                                  srt.completion_time_utc as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'milestone'
                                                  ) pts
                                                  left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                                  and pts.task_instance_id = srt.task_instance_id
                                                  and pts.study_version_id = srt.study_version_id
                                                where
                                                  pts.task_type IN ('survey', 'epro')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? `WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          milestone_id
                                      ) T2
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          milestone_id,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              PTS.milestone_id,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ppa.id ar_id,
                                                  pts.milestone_id,
                                                  'visits' as response_type,
                                                  ppa.status as visit_status,
                                                  ppa.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'milestone'
                                                  ) pts
                                                  left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                                  and pts.task_instance_id = ppa.task_instanceuuid
                                                  and pts.study_version_id = ppa.study_version_id
                                                  and (ppa.visit_id IS NOT NULL)
                                                WHERE
                                                  pts.task_type IN ('telehealth')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? `WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          milestone_id
                                      ) T3
                                  ) F1
                                where
                                  participant_start_date is not null
                                group by
                                  participant_id,
                                  milestone_id
                              ) QD
                              left join (
                                select
                                  milestone_id,
                                  display_name,
                                  sort_order
                                from
                                  research.milestones
                              ) RM ON QD.milestone_id = RM.milestone_id
                            order by
                              RM.sort_order
                          ) QM
                        group by
                          month,
                          year,
                          sort_order
                      ) QMG ON QD.join_date = QMG.join_date
                      and QD.sort_order = QMG.sort_order
                      LEFT JOIN (
                        select
                          min(start_day) as start_day,
                          milestone_id
                        from
                          research_response.participant_task_schedule
                        where
                          study_id = ?
                        group by
                          milestone_id
                      ) QJD ON QD.milestone_id = QJD.milestone_id
                      LEFT JOIN (
                        select
                          join_date,
                          count(participant_id) n_participant
                        from
                          (
                            select
                              participant_id,
                              DATE_FORMAT(DATE(modified_time), '%b-%Y') as join_date
                            from
                              research.participant_status_history
                            where
                              new_status in ('ACTIVE')
                              and study_id = ?
                          ) t1
                        group by
                          join_date
                      ) QPC ON DATE_FORMAT(QD.join_date, '%b-%Y') = QPC.join_date
                    ORDER BY
                      QD.date_id,
                      QD.sort_order
                  ) QFC
                where
                  QFC.bucket_name = 'Users'
                UNION
                select
                  'All Users' join_date,
                  bucket_name,
                  bucket_order,
                  n_participant,
                  AVG(participation_rate) participation_rate
                from
                  (
                    select
                      QD.date_id,
                      DATE_FORMAT(QD.join_date, '%b-%Y') as join_date,
                      QD.milestone_name bucket_name,
                      QD.sort_order + 2 as bucket_order,
                      IFNULL(QMG.n_participant, 0) as n_participant,
                      case when QD.milestone_name = 'Users' then IFNULL(QPC.n_participant, 0) when IFNULL(QPC.n_participant, 0) = 0 then null when QD.milestone_name = 'DAY 0'
                      and IFNULL(QPC.n_participant, 0) > 0 then 1 WHEN QMG.participation_rate is not null then QMG.participation_rate WHEN (
                        DATE_ADD(QD.join_date, INTERVAL QJD.start_day DAY) <= NOW()
                      )
                      AND QMG.participation_rate is null then 0 WHEN DATE_ADD(QD.join_date, INTERVAL QJD.start_day DAY) > NOW() then null else null end as participation_rate
                    from
                      (
                        select
                          *
                        from
                          (
                            select
                              t1.join_date,
                              (@csum := @csum + 1) as date_id
                            from
                              (
                                select
                                  DISTINCT DATE(
                                    CONCAT(
                                      EXTRACT(
                                        year
                                        from
                                          gen_date
                                      ),
                                      '-',
                                      EXTRACT(
                                        MONTH
                                        from
                                          gen_date
                                      ),
                                      '-01'
                                    )
                                  ) as join_date
                                from
                                  (
                                    select
                                      adddate(
                                        '1970-01-01',
                                        t4 * 10000 + t3 * 1000 + t2 * 100 + t1 * 10 + t0
                                      ) gen_date
                                    from
                                      (
                                        select
                                          0 t0
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t0,
                                      (
                                        select
                                          0 t1
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t1,
                                      (
                                        select
                                          0 t2
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t2,
                                      (
                                        select
                                          0 t3
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t3,
                                      (
                                        select
                                          0 t4
                                        union
                                        select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t4
                                  ) v
                                where
                                  gen_date between (
                                    select
                                      study_start_date
                                    from
                                      research.study_meta_data
                                    where
                                      id = ?
                                  )
                                  and NOW()
                              ) t1
                              CROSS JOIN (
                                SELECT
                                  @csum := 0
                              ) AS dummy
                          ) QD,
                          (
                            select
                              'xy' milestone_id,
                              'Users' as milestone_name,
                              -2 as sort_order
                            UNION
                            select
                              'x' milestone_id,
                              'DAY 0' as milestone_name,
                              -1 as sort_order
                            UNION
                              (
                                select
                                  milestone_id,
                                  display_name as milestone_name,
                                  sort_order
                                from
                                  research.milestones
                                where
                                  study_id = ?
                              )
                          ) QM
                      ) QD
                      LEFT JOIN (
                        select
                          DATE(CONCAT(year, '-', month, '-01')) as join_date,
                          milestone_name,
                          milestone_id,
                          QM.sort_order,
                          count(participant_id) as n_participant,
                          SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate
                        from
                          (
                            select
                              QD.study_id,
                              QD.country_id,
                              QD.countryName,
                              QD.site_id,
                              QD.siteName,
                              QD.milestone_id,
                              QD.nTotalCompleted,
                              QD.nTotalMissing,
                              QD.participant_id,
                              QD.participant_start_date,
                              QD.is_participate,
                              RM.display_name as milestone_name,
                              RM.sort_order,
                              EXTRACT(
                                MONTH
                                from
                                  QD.participant_start_date
                              ) AS 'month',
                              EXTRACT(
                                year
                                from
                                  QD.participant_start_date
                              ) AS year
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name as countryName,
                                  site_id,
                                  participant_start_date,
                                  site_name as siteName,
                                  milestone_id,
                                  SUM(n_total_completed) nTotalCompleted,
                                  SUM(n_total_missing) nTotalMissing,
                                  participant_id,
                                  case when SUM(n_total_completed) > 0 then 1 else 0 end as is_participate
                                from
                                  (
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          milestone_id,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              PTS.milestone_id,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ar.id ar_id,
                                                  pts.milestone_id,
                                                  'activity' as response_type,
                                                  '' as visit_status,
                                                  ar.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'milestone'
                                                  ) pts
                                                  left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                                  and pts.task_instance_id = ar.task_instance_id
                                                  and pts.study_version_id = ar.study_version_id
                                                where
                                                  pts.task_type IN ('activity')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? `WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          milestone_id
                                      ) T1
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          milestone_id,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              PTS.milestone_id,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                SELECT
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  srt.id ar_id,
                                                  pts.milestone_id,
                                                  'survey' as response_type,
                                                  '' as visit_status,
                                                  srt.completion_time_utc as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'milestone'
                                                  ) pts
                                                  left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                                  and pts.task_instance_id = srt.task_instance_id
                                                  and pts.study_version_id = srt.study_version_id
                                                where
                                                  pts.task_type IN ('survey', 'epro')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? `WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          milestone_id
                                      ) T2
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          milestone_id,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              PTS.milestone_id,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ppa.id ar_id,
                                                  pts.milestone_id,
                                                  'visits' as response_type,
                                                  ppa.status as visit_status,
                                                  ppa.end_time as completion_date
                                                FROM
                                                  (
                                                  select
                                                    *
                                                  from
                                                    research_response.participant_task_schedule
                                                  where
                                                    study_id = ?
                                                    and schedule_type = 'milestone'
                                                ) pts
                                                left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                                and pts.task_instance_id = ppa.task_instanceuuid
                                                and pts.study_version_id = ppa.study_version_id
                                                and (ppa.visit_id IS NOT NULL)
                                              WHERE
                                                pts.task_type IN ('telehealth')
                                            ) PTS ON PSC.participant_id = PTS.participant_id
                                            and PSC.study_id = PTS.study_id
                                          ${params.fromDate ? `WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                        ) t1
                                      group by
                                        participant_id,
                                        milestone_id
                                    ) T3
                                ) F1
                              where
                                participant_start_date is not null
                              group by
                                participant_id,
                                milestone_id
                            ) QD
                            left join (
                              select
                                milestone_id,
                                display_name,
                                sort_order
                              from
                                research.milestones
                            ) RM ON QD.milestone_id = RM.milestone_id
                          order by
                            RM.sort_order
                        ) QM
                      group by
                        month,
                        year,
                        sort_order
                    ) QMG ON QD.join_date = QMG.join_date
                    and QD.sort_order = QMG.sort_order
                    LEFT JOIN (
                      select
                        min(start_day) as start_day,
                        milestone_id
                      from
                        research_response.participant_task_schedule
                      where
                        study_id = ?
                      group by
                        milestone_id
                    ) QJD ON QD.milestone_id = QJD.milestone_id
                    LEFT JOIN (
                      select
                        join_date,
                        count(participant_id) n_participant
                      from
                        (
                          select
                            participant_id,
                            DATE_FORMAT(DATE(modified_time), '%b-%Y') as join_date
                          from
                            research.participant_status_history
                          where
                            new_status in ('ACTIVE')
                            and study_id = ?
                        ) t1
                      group by
                        join_date
                    ) QPC ON DATE_FORMAT(QD.join_date, '%b-%Y') = QPC.join_date
                  ORDER BY
                    QD.date_id,
                    QD.sort_order
                ) QFC
              where
                QFC.bucket_name != 'Users'
              group by
                bucket_name
              order by
                bucket_order
            ) t1
        ) t2
      ORDER BY
        t2.date_id,
        t2.bucket_order
      
      `;

      console.log(`getMilestoneCohortData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getMilestoneCohortData:', error);
      throw error;
    }
  }

  async getCustomCohortData(params) {
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      for (let index = 0; index < 30; index++) {
        bindingParams.push(params.studyId)
      }

      const querySql = `
        set @csum := 0;
        select
          join_date,
          bucket_name,
          bucket_order + 1 bucket_order,
          n_participant,
          participation_rate
        from
          (
            select
              date_id,
              join_date,
              bucket_name,
              bucket_order + 1 bucket_order,
              n_participant,
              participation_rate
            from
              (
                select
                  QD.date_id,
                  DATE_FORMAT(QD.join_date, '%b-%Y') as join_date,
                  QD.bucket_day as bucket_name,
                  QD.sort_order as bucket_order,
                  IFNULL(QMG.n_participant, 0) as n_participant,
                  case when QD.bucket_day = 'Users' then IFNULL(QPC.n_participant, 0) when IFNULL(QPC.n_participant, 0) = 0 then null when QD.bucket_day = 'DAY 0'
                  and IFNULL(QPC.n_participant, 0) > 0 then 1 WHEN QMG.participation_rate is not null then QMG.participation_rate WHEN (
                    DATE_ADD(QD.join_date, INTERVAL QD.start_day DAY) <= NOW()
                  )
                  AND QMG.participation_rate is null then 0 WHEN DATE_ADD(QD.join_date, INTERVAL QD.start_day DAY) > NOW() then null else null end as participation_rate
                from
                  (
                    select
                      *
                    from
                      (
                        select
                          t1.join_date,
                          (@csum := @csum + 1) as date_id
                        from
                          (
                            select
                              DISTINCT DATE(
                                CONCAT(
                                  EXTRACT(
                                    year
                                    from
                                      gen_date
                                  ),
                                  '-',
                                  EXTRACT(
                                    MONTH
                                    from
                                      gen_date
                                  ),
                                  '-01'
                                )
                              ) as join_date
                            from
                              (
                                select
                                  adddate(
                                    '1970-01-01',
                                    t4 * 10000 + t3 * 1000 + t2 * 100 + t1 * 10 + t0
                                  ) gen_date
                                from
                                  (
                                    select
                                      0 t0
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t0,
                                  (
                                    select
                                      0 t1
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t1,
                                  (
                                    select
                                      0 t2
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t2,
                                  (
                                    select
                                      0 t3
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t3,
                                  (
                                    select
                                      0 t4
                                    union select 1
                                    union select 2
                                    union select 3
                                    union select 4
                                    union select 5
                                    union select 6
                                    union select 7
                                    union select 8
                                    union select 9
                                  ) t4
                              ) v
                            where
                              gen_date between (
                                select
                                  study_start_date
                                from
                                  research.study_meta_data
                                where
                                  id = ?
                              )
                              and NOW()
                          ) t1
                          CROSS JOIN (
                            SELECT
                              @csum := 0
                          ) AS dummy
                      ) QD,
                      (
                        select
                          'Users' as bucket_day,
                          -1 as sort_order,
                          -1 as start_day
                        UNION
                        select
                          'DAY 0' as bucket_day,
                          0 as sort_order,
                          0 as start_day
                        UNION
                        select
                          DISTINCT CONCAT(
                            'DAY ',
                            FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                          ) as bucket_day,
                          FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                          FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                        from
                          (
                            Select
                              start_day
                            from
                              research_response.participant_task_schedule
                            where
                              study_id = ?
                              and schedule_type = 'custom'
                          ) t1
                      ) QM
                  ) QD
                  LEFT JOIN (
                    select
                      DATE(CONCAT(year, '-', month, '-01')) as join_date,
                      bucket_name,
                      QM.sort_order,
                      count(participant_id) as n_participant,
                      SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate
                    from
                      (
                        select
                          QD.study_id,
                          QD.country_id,
                          QD.countryName,
                          QD.site_id,
                          QD.siteName,
                          QD.nTotalCompleted,
                          QD.nTotalMissing,
                          QD.participant_id,
                          QD.participant_start_date,
                          QD.is_participate,
                          QD.bucket_day bucket_name,
                          RM.sort_order,
                          EXTRACT(
                            MONTH
                            from
                              QD.participant_start_date
                          ) AS 'month',
                          EXTRACT(
                            year
                            from
                              QD.participant_start_date
                          ) AS year
                        from
                          (
                            select
                              study_id,
                              country_id,
                              country_name as countryName,
                              site_id,
                              participant_start_date,
                              site_name as siteName,
                              bucket_day,
                              SUM(n_total_completed) nTotalCompleted,
                              SUM(n_total_missing) nTotalMissing,
                              participant_id,
                              case when SUM(n_total_completed) > 0 then 1 else 0 end as is_participate
                            from
                              (
                                select
                                  *
                                from
                                  (
                                    select
                                      study_id,
                                      country_id,
                                      country_name,
                                      site_id,
                                      participant_start_date,
                                      site_name,
                                      activity_name,
                                      participant_id,
                                      bucket_day,
                                      count(case when status = 'Complete' then 1 end) n_total_completed,
                                      count(case when status = 'Missed' then 1 end) n_total_missing
                                    from
                                      (
                                        select
                                          PSC.participant_id,
                                          PSC.site_name,
                                          PSC.site_id,
                                          participant_start_date,
                                          PSC.study_name,
                                          PSC.study_id,
                                          PSC.country_id,
                                          PSC.country_name,
                                          PTS.task_title as activity_name,
                                          CONCAT(
                                            'DAY ',
                                            FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                          ) as bucket_day,
                                          CASE WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                          AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' END AS status
                                        from
                                          (
                                            select
                                              *
                                            from
                                              research_analytics.participant_site_country
                                            where
                                              study_id = ?
                                              ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ) PSC
                                          INNER JOIN (
                                            select
                                              pts.participant_id,
                                              PTS.study_id,
                                              pts.task_title,
                                              pts.created_date,
                                              pts.start_day,
                                              pts.end_day,
                                              ar.id ar_id,
                                              'activity' as response_type,
                                              '' as visit_status,
                                              ar.end_time as completion_date
                                            FROM
                                              (
                                                select
                                                  *
                                                from
                                                  research_response.participant_task_schedule
                                                where
                                                  study_id = ?
                                                  and schedule_type = 'custom'
                                              ) pts
                                              left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                              and pts.task_instance_id = ar.task_instance_id
                                              and pts.study_version_id = ar.study_version_id
                                            where
                                              pts.task_type IN ('activity')
                                          ) PTS ON PSC.participant_id = PTS.participant_id
                                          and PSC.study_id = PTS.study_id
                                        ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                      ) t1
                                    group by
                                      participant_id,
                                      bucket_day
                                  ) T1
                                UNION
                                select
                                  *
                                from
                                  (
                                    select
                                      study_id,
                                      country_id,
                                      country_name,
                                      site_id,
                                      participant_start_date,
                                      site_name,
                                      activity_name,
                                      participant_id,
                                      bucket_day,
                                      count(case when status = 'Complete' then 1 end) n_total_completed,
                                      count(case when status = 'Missed' then 1 end) n_total_missing
                                    from
                                      (
                                        select
                                          PSC.participant_id,
                                          PSC.site_name,
                                          PSC.site_id,
                                          participant_start_date,
                                          PSC.study_name,
                                          PSC.study_id,
                                          PSC.country_id,
                                          PSC.country_name,
                                          PTS.task_title as activity_name,
                                          CONCAT(
                                            'DAY ',
                                            FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                          ) as bucket_day,
                                          CASE WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                          AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' END AS status
                                        from
                                          (
                                            select
                                              *
                                            from
                                              research_analytics.participant_site_country
                                            where
                                              study_id = ?
                                              ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ) PSC
                                          INNER JOIN (
                                            SELECT
                                              pts.participant_id,
                                              PTS.study_id,
                                              pts.task_title,
                                              pts.created_date,
                                              pts.start_day,
                                              pts.end_day,
                                              srt.id ar_id,
                                              'survey' as response_type,
                                              '' as visit_status,
                                              srt.completion_time_utc as completion_date
                                            FROM
                                              (
                                                select
                                                  *
                                                from
                                                  research_response.participant_task_schedule
                                                where
                                                  study_id = ?
                                                  and schedule_type = 'custom'
                                              ) pts
                                              left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                              and pts.task_instance_id = srt.task_instance_id
                                              and pts.study_version_id = srt.study_version_id
                                            where
                                              pts.task_type IN ('survey', 'epro')
                                          ) PTS ON PSC.participant_id = PTS.participant_id
                                          and PSC.study_id = PTS.study_id
                                        ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                      ) t1
                                    group by
                                      participant_id,
                                      bucket_day
                                  ) T2
                                UNION
                                select
                                  *
                                from
                                  (
                                    select
                                      study_id,
                                      country_id,
                                      country_name,
                                      site_id,
                                      participant_start_date,
                                      site_name,
                                      activity_name,
                                      participant_id,
                                      bucket_day,
                                      count(case when status = 'Complete' then 1 end) n_total_completed,
                                      count(case when status = 'Missed' then 1 end) n_total_missing
                                    from
                                      (
                                        select
                                          PSC.participant_id,
                                          PSC.site_name,
                                          PSC.site_id,
                                          participant_start_date,
                                          PSC.study_name,
                                          PSC.study_id,
                                          PSC.country_id,
                                          PSC.country_name,
                                          PTS.task_title as activity_name,
                                          CONCAT(
                                            'DAY ',
                                            FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                          ) as bucket_day,
                                          CASE WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                          AND PTS.ar_id IS NOT NULL
                                          AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                          AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day = (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day < (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          )
                                          AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                          AND PTS.end_day >= (
                                            TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                          ) THEN 'Scheduled' END AS status
                                        from
                                          (
                                            select
                                              *
                                            from
                                              research_analytics.participant_site_country
                                            where
                                              study_id = ?
                                              ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ) PSC
                                          INNER JOIN (
                                            select
                                              pts.participant_id,
                                              PTS.study_id,
                                              pts.task_title,
                                              pts.created_date,
                                              pts.start_day,
                                              pts.end_day,
                                              ppa.id ar_id,
                                              'visits' as response_type,
                                              ppa.status as visit_status,
                                              ppa.end_time as completion_date
                                            FROM
                                              (
                                                select
                                                  *
                                                from
                                                  research_response.participant_task_schedule
                                                where
                                                  study_id = ?
                                                  and schedule_type = 'custom'
                                              ) pts
                                              left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                              and pts.task_instance_id = ppa.task_instanceuuid
                                              and pts.study_version_id = ppa.study_version_id
                                              and (ppa.visit_id IS NOT NULL)
                                            WHERE
                                              pts.task_type IN ('telehealth')
                                          ) PTS ON PSC.participant_id = PTS.participant_id
                                          and PSC.study_id = PTS.study_id
                                        ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                      ) t1
                                    group by
                                      participant_id,
                                      bucket_day
                                  ) T3
                              ) F1
                            where
                              participant_start_date is not null
                            group by
                              participant_id,
                              bucket_day
                          ) QD
                          left join (
                            select
                              'Users' as bucket_day,
                              -1 as sort_order,
                              -1 as start_day
                            UNION
                            select
                              'DAY 0' as bucket_day,
                              0 as sort_order,
                              0 as start_day
                            UNION
                            select
                              DISTINCT CONCAT(
                                'DAY ',
                                FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                              ) as bucket_day,
                              FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                              FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                            from
                              (
                                Select
                                  start_day
                                from
                                  research_response.participant_task_schedule
                                where
                                  study_id = ?
                                  and schedule_type = 'custom'
                              ) t1
                          ) RM ON QD.bucket_day = RM.bucket_day
                        order by
                          RM.sort_order
                      ) QM
                    group by
                      month,
                      year,
                      sort_order
                  ) QMG ON QD.join_date = QMG.join_date
                  and QD.sort_order = QMG.sort_order
                  LEFT JOIN (
                    select
                      join_date,
                      count(participant_id) n_participant
                    from
                      (
                        select
                          participant_id,
                          DATE_FORMAT(DATE(modified_time), '%b-%Y') as join_date
                        from
                          research.participant_status_history
                        where
                          new_status in ('ACTIVE')
                          and study_id = ?
                      ) t1
                    group by
                      join_date
                  ) QPC ON DATE_FORMAT(QD.join_date, '%b-%Y') = QPC.join_date
                ORDER BY
                  QD.date_id,
                  QD.sort_order
              ) QFC
            UNION
            select
              999 date_id,
              join_date,
              bucket_name,
              bucket_order + 1 bucket_order,
              n_participant,
              participation_rate
            from
              (
                select
                  'All Users' join_date,
                  bucket_name,
                  999 bucket_order,
                  n_participant,
                  SUM(participation_rate) participation_rate
                from
                  (
                    select
                      QD.date_id,
                      DATE_FORMAT(QD.join_date, '%b-%Y') as join_date,
                      QD.bucket_day as bucket_name,
                      QD.sort_order as bucket_order,
                      IFNULL(QMG.n_participant, 0) as n_participant,
                      case when QD.bucket_day = 'Users' then IFNULL(QPC.n_participant, 0) when IFNULL(QPC.n_participant, 0) = 0 then null when QD.bucket_day = 'DAY 0'
                      and IFNULL(QPC.n_participant, 0) > 0 then 1 WHEN QMG.participation_rate is not null then QMG.participation_rate WHEN (
                        DATE_ADD(QD.join_date, INTERVAL QD.start_day DAY) <= NOW()
                      )
                      AND QMG.participation_rate is null then 0 WHEN DATE_ADD(QD.join_date, INTERVAL QD.start_day DAY) > NOW() then null else null end as participation_rate
                    from
                      (
                        select
                          *
                        from
                          (
                            select
                              t1.join_date,
                              (@csum := @csum + 1) as date_id
                            from
                              (
                                select
                                  DISTINCT DATE(
                                    CONCAT(
                                      EXTRACT(
                                        year
                                        from
                                          gen_date
                                      ),
                                      '-',
                                      EXTRACT(
                                        MONTH
                                        from
                                          gen_date
                                      ),
                                      '-01'
                                    )
                                  ) as join_date
                                from
                                  (
                                    select
                                      adddate(
                                        '1970-01-01',
                                        t4 * 10000 + t3 * 1000 + t2 * 100 + t1 * 10 + t0
                                      ) gen_date
                                    from
                                      (
                                        select
                                          0 t0
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t0,
                                      (
                                        select
                                          0 t1
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t1,
                                      (
                                        select
                                          0 t2
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t2,
                                      (
                                        select
                                          0 t3
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t3,
                                      (
                                        select
                                          0 t4
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t4
                                  ) v
                                where
                                  gen_date between (
                                    select
                                      study_start_date
                                    from
                                      research.study_meta_data
                                    where
                                      id = ?
                                  )
                                  and NOW()
                              ) t1
                              CROSS JOIN (
                                SELECT
                                  @csum := 0
                              ) AS dummy
                          ) QD,
                          (
                            select
                              'Users' as bucket_day,
                              -1 as sort_order,
                              -1 as start_day
                            UNION
                            select
                              'DAY 0' as bucket_day,
                              0 as sort_order,
                              0 as start_day
                            UNION
                            select
                              DISTINCT CONCAT(
                                'DAY ',
                                FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                              ) as bucket_day,
                              FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                              FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                            from
                              (
                                Select
                                  start_day
                                from
                                  research_response.participant_task_schedule
                                where
                                  study_id = ?
                                  and schedule_type = 'custom'
                              ) t1
                          ) QM
                      ) QD
                      LEFT JOIN (
                        select
                          DATE(CONCAT(year, '-', month, '-01')) as join_date,
                          bucket_name,
                          QM.sort_order,
                          count(participant_id) as n_participant,
                          SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate
                        from
                          (
                            select
                              QD.study_id,
                              QD.country_id,
                              QD.countryName,
                              QD.site_id,
                              QD.siteName,
                              QD.nTotalCompleted,
                              QD.nTotalMissing,
                              QD.participant_id,
                              QD.participant_start_date,
                              QD.is_participate,
                              QD.bucket_day bucket_name,
                              RM.sort_order,
                              EXTRACT(
                                MONTH
                                from
                                  QD.participant_start_date
                              ) AS 'month',
                              EXTRACT(
                                year
                                from
                                  QD.participant_start_date
                              ) AS year
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name as countryName,
                                  site_id,
                                  participant_start_date,
                                  site_name as siteName,
                                  bucket_day,
                                  SUM(n_total_completed) nTotalCompleted,
                                  SUM(n_total_missing) nTotalMissing,
                                  participant_id,
                                  case when SUM(n_total_completed) > 0 then 1 else 0 end as is_participate
                                from
                                  (
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          bucket_day,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              CONCAT(
                                                'DAY ',
                                                FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                              ) as bucket_day,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ar.id ar_id,
                                                  'activity' as response_type,
                                                  '' as visit_status,
                                                  ar.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'custom'
                                                  ) pts
                                                  left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                                  and pts.task_instance_id = ar.task_instance_id
                                                  and pts.study_version_id = ar.study_version_id
                                                where
                                                  pts.task_type IN ('activity')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          bucket_day
                                      ) T1
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          bucket_day,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              CONCAT(
                                                'DAY ',
                                                FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                              ) as bucket_day,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                SELECT
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  srt.id ar_id,
                                                  'survey' as response_type,
                                                  '' as visit_status,
                                                  srt.completion_time_utc as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'custom'
                                                  ) pts
                                                  left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                                  and pts.task_instance_id = srt.task_instance_id
                                                  and pts.study_version_id = srt.study_version_id
                                                where
                                                  pts.task_type IN ('survey', 'epro')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          bucket_day
                                      ) T2
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          bucket_day,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              CONCAT(
                                                'DAY ',
                                                FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                              ) as bucket_day,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ppa.id ar_id,
                                                  'visits' as response_type,
                                                  ppa.status as visit_status,
                                                  ppa.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'custom'
                                                  ) pts
                                                  left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                                  and pts.task_instance_id = ppa.task_instanceuuid
                                                  and pts.study_version_id = ppa.study_version_id
                                                  and (ppa.visit_id IS NOT NULL)
                                                WHERE
                                                  pts.task_type IN ('telehealth')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          bucket_day
                                      ) T3
                                  ) F1
                                where
                                  participant_start_date is not null
                                group by
                                  participant_id,
                                  bucket_day
                              ) QD
                              left join (
                                select
                                  'Users' as bucket_day,
                                  -1 as sort_order,
                                  -1 as start_day
                                UNION
                                select
                                  'DAY 0' as bucket_day,
                                  0 as sort_order,
                                  0 as start_day
                                UNION
                                select
                                  DISTINCT CONCAT(
                                    'DAY ',
                                    FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                                  ) as bucket_day,
                                  FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                                  FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                                from
                                  (
                                    Select
                                      start_day
                                    from
                                      research_response.participant_task_schedule
                                    where
                                      study_id = ?
                                      and schedule_type = 'custom'
                                  ) t1
                              ) RM ON QD.bucket_day = RM.bucket_day
                            order by
                              RM.sort_order
                          ) QM
                        group by
                          month,
                          year,
                          sort_order
                      ) QMG ON QD.join_date = QMG.join_date
                      and QD.sort_order = QMG.sort_order
                      LEFT JOIN (
                        select
                          join_date,
                          count(participant_id) n_participant
                        from
                          (
                            select
                              participant_id,
                              DATE_FORMAT(DATE(modified_time), '%b-%Y') as join_date
                            from
                              research.participant_status_history
                            where
                              new_status in ('ACTIVE')
                              and study_id = ?
                          ) t1
                        group by
                          join_date
                      ) QPC ON DATE_FORMAT(QD.join_date, '%b-%Y') = QPC.join_date
                    ORDER BY
                      QD.date_id,
                      QD.sort_order
                  ) QFC
                where
                  QFC.bucket_name = 'Users'
                UNION
                select
                  'All Users' join_date,
                  bucket_name,
                  bucket_order,
                  n_participant,
                  AVG(participation_rate) participation_rate
                from
                  (
                    select
                      QD.date_id,
                      DATE_FORMAT(QD.join_date, '%b-%Y') as join_date,
                      QD.bucket_day as bucket_name,
                      QD.sort_order as bucket_order,
                      IFNULL(QMG.n_participant, 0) as n_participant,
                      case when QD.bucket_day = 'Users' then IFNULL(QPC.n_participant, 0) when IFNULL(QPC.n_participant, 0) = 0 then null when QD.bucket_day = 'DAY 0'
                      and IFNULL(QPC.n_participant, 0) > 0 then 1 WHEN QMG.participation_rate is not null then QMG.participation_rate WHEN (
                        DATE_ADD(QD.join_date, INTERVAL QD.start_day DAY) <= NOW()
                      )
                      AND QMG.participation_rate is null then 0 WHEN DATE_ADD(QD.join_date, INTERVAL QD.start_day DAY) > NOW() then null else null end as participation_rate
                    from
                      (
                        select
                          *
                        from
                          (
                            select
                              t1.join_date,
                              (@csum := @csum + 1) as date_id
                            from
                              (
                                select
                                  DISTINCT DATE(
                                    CONCAT(
                                      EXTRACT(
                                        year
                                        from
                                          gen_date
                                      ),
                                      '-',
                                      EXTRACT(
                                        MONTH
                                        from
                                          gen_date
                                      ),
                                      '-01'
                                    )
                                  ) as join_date
                                from
                                  (
                                    select
                                      adddate(
                                        '1970-01-01',
                                        t4 * 10000 + t3 * 1000 + t2 * 100 + t1 * 10 + t0
                                      ) gen_date
                                    from
                                      (
                                        select
                                          0 t0
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t0,
                                      (
                                        select
                                          0 t1
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t1,
                                      (
                                        select
                                          0 t2
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t2,
                                      (
                                        select
                                          0 t3
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t3,
                                      (
                                        select
                                          0 t4
                                        union select 1
                                        union select 2
                                        union select 3
                                        union select 4
                                        union select 5
                                        union select 6
                                        union select 7
                                        union select 8
                                        union select 9
                                      ) t4
                                  ) v
                                where
                                  gen_date between (
                                    select
                                      study_start_date
                                    from
                                      research.study_meta_data
                                    where
                                      id = ?
                                  )
                                  and NOW()
                              ) t1
                              CROSS JOIN (
                                SELECT
                                  @csum := 0
                              ) AS dummy
                          ) QD,
                          (
                            select
                              'Users' as bucket_day,
                              -1 as sort_order,
                              -1 as start_day
                            UNION
                            select
                              'DAY 0' as bucket_day,
                              0 as sort_order,
                              0 as start_day
                            UNION
                            select
                              DISTINCT CONCAT(
                                'DAY ',
                                FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                              ) as bucket_day,
                              FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                              FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                            from
                              (
                                Select
                                  start_day
                                from
                                  research_response.participant_task_schedule
                                where
                                  study_id = ?
                                  and schedule_type = 'custom'
                              ) t1
                          ) QM
                      ) QD
                      LEFT JOIN (
                        select
                          DATE(CONCAT(year, '-', month, '-01')) as join_date,
                          bucket_name,
                          QM.sort_order,
                          count(participant_id) as n_participant,
                          SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate
                        from
                          (
                            select
                              QD.study_id,
                              QD.country_id,
                              QD.countryName,
                              QD.site_id,
                              QD.siteName,
                              QD.nTotalCompleted,
                              QD.nTotalMissing,
                              QD.participant_id,
                              QD.participant_start_date,
                              QD.is_participate,
                              QD.bucket_day bucket_name,
                              RM.sort_order,
                              EXTRACT(
                                MONTH
                                from
                                  QD.participant_start_date
                              ) AS 'month',
                              EXTRACT(
                                year
                                from
                                  QD.participant_start_date
                              ) AS year
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name as countryName,
                                  site_id,
                                  participant_start_date,
                                  site_name as siteName,
                                  bucket_day,
                                  SUM(n_total_completed) nTotalCompleted,
                                  SUM(n_total_missing) nTotalMissing,
                                  participant_id,
                                  case when SUM(n_total_completed) > 0 then 1 else 0 end as is_participate
                                from
                                  (
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          bucket_day,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              CONCAT(
                                                'DAY ',
                                                FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                              ) as bucket_day,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ar.id ar_id,
                                                  'activity' as response_type,
                                                  '' as visit_status,
                                                  ar.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'custom'
                                                  ) pts
                                                  left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                                  and pts.task_instance_id = ar.task_instance_id
                                                  and pts.study_version_id = ar.study_version_id
                                                where
                                                  pts.task_type IN ('activity')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          bucket_day
                                      ) T1
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          bucket_day,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              CONCAT(
                                                'DAY ',
                                                FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                              ) as bucket_day,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                SELECT
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  srt.id ar_id,
                                                  'survey' as response_type,
                                                  '' as visit_status,
                                                  srt.completion_time_utc as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'custom'
                                                  ) pts
                                                  left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                                  and pts.task_instance_id = srt.task_instance_id
                                                  and pts.study_version_id = srt.study_version_id
                                                where
                                                  pts.task_type IN ('survey', 'epro')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          bucket_day
                                      ) T2
                                    UNION
                                    select
                                      *
                                    from
                                      (
                                        select
                                          study_id,
                                          country_id,
                                          country_name,
                                          site_id,
                                          participant_start_date,
                                          site_name,
                                          activity_name,
                                          participant_id,
                                          bucket_day,
                                          count(case when status = 'Complete' then 1 end) n_total_completed,
                                          count(case when status = 'Missed' then 1 end) n_total_missing
                                        from
                                          (
                                            select
                                              PSC.participant_id,
                                              PSC.site_name,
                                              PSC.site_id,
                                              participant_start_date,
                                              PSC.study_name,
                                              PSC.study_id,
                                              PSC.country_id,
                                              PSC.country_name,
                                              PTS.task_title as activity_name,
                                              CONCAT(
                                                'DAY ',
                                                FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                              ) as bucket_day,
                                              CASE WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                              AND PTS.ar_id IS NOT NULL
                                              AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                              AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day = (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day < (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              )
                                              AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                              AND PTS.end_day >= (
                                                TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                              ) THEN 'Scheduled' END AS status
                                            from
                                              (
                                                select
                                                  *
                                                from
                                                  research_analytics.participant_site_country
                                                where
                                                  study_id = ?
                                                  ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                              ) PSC
                                              INNER JOIN (
                                                select
                                                  pts.participant_id,
                                                  PTS.study_id,
                                                  pts.task_title,
                                                  pts.created_date,
                                                  pts.start_day,
                                                  pts.end_day,
                                                  ppa.id ar_id,
                                                  'visits' as response_type,
                                                  ppa.status as visit_status,
                                                  ppa.end_time as completion_date
                                                FROM
                                                  (
                                                    select
                                                      *
                                                    from
                                                      research_response.participant_task_schedule
                                                    where
                                                      study_id = ?
                                                      and schedule_type = 'custom'
                                                  ) pts
                                                  left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                                  and pts.task_instance_id = ppa.task_instanceuuid
                                                  and pts.study_version_id = ppa.study_version_id
                                                  and (ppa.visit_id IS NOT NULL)
                                                WHERE
                                                  pts.task_type IN ('telehealth')
                                              ) PTS ON PSC.participant_id = PTS.participant_id
                                              and PSC.study_id = PTS.study_id
                                            ${params.fromDate ? ` WHERE PSC.enrollment_date between '${params.fromDate}' and '${params.toDate}'` : ''}
                                          ) t1
                                        group by
                                          participant_id,
                                          bucket_day
                                      ) T3
                                  ) F1
                                where
                                  participant_start_date is not null
                                group by
                                  participant_id,
                                  bucket_day
                              ) QD
                              left join (
                                select
                                  'Users' as bucket_day,
                                  -1 as sort_order,
                                  -1 as start_day
                                UNION
                                select
                                  'DAY 0' as bucket_day,
                                  0 as sort_order,
                                  0 as start_day
                                UNION
                                select
                                  DISTINCT CONCAT(
                                    'DAY ',
                                    FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                                  ) as bucket_day,
                                  FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                                  FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                                from
                                  (
                                    Select
                                      start_day
                                    from
                                      research_response.participant_task_schedule
                                    where
                                      study_id = ?
                                      and schedule_type = 'custom'
                                  ) t1
                              ) RM ON QD.bucket_day = RM.bucket_day
                            order by
                              RM.sort_order
                          ) QM
                        group by
                          month,
                          year,
                          sort_order
                      ) QMG ON QD.join_date = QMG.join_date
                      and QD.sort_order = QMG.sort_order
                      LEFT JOIN (
                        select
                          join_date,
                          count(participant_id) n_participant
                        from
                          (
                            select
                              participant_id,
                              DATE_FORMAT(DATE(modified_time), '%b-%Y') as join_date
                            from
                              research.participant_status_history
                            where
                              new_status in ('ACTIVE')
                              and study_id = ?
                          ) t1
                        group by
                          join_date
                      ) QPC ON DATE_FORMAT(QD.join_date, '%b-%Y') = QPC.join_date
                    ORDER BY
                      QD.date_id,
                      QD.sort_order
                  ) QFC
                where
                  QFC.bucket_name != 'Users'
                group by
                  bucket_name
                order by
                  bucket_order
              ) t1
          ) t2
        ORDER BY
          t2.date_id,
          t2.bucket_order
    
      `;
      console.log(`getCustomCohortData query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getCustomCohortData:', error);
      throw error;
    }
  }

  async getRetentionScoreMilestone(params) {
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      for (let index = 0; index < 12; index++) {
        bindingParams.push(params.studyId)
      }

      const querySql = `
        select
          AVG(t1.participation_rate) * 10 as retention_score,
          AVG(t1.participation_rate_prev1week) * 10 as retention_score_prev1week
        from
          (
            select
              join_date,
              sort_order,
              participation_rate,
              participation_rate_prev1week
            from
              (
                select
                  DATE(CONCAT(year, '-', month, '-01')) as join_date,
                  milestone_name,
                  milestone_id,
                  QM.sort_order,
                  count(participant_id) as n_participant,
                  SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate,
                  SUM(nTotalCompletedPrev1Week) /(
                    SUM(nTotalCompletedPrev1Week) + SUM(nTotalMissingPrev1Week)
                  ) as participation_rate_prev1week
                from
                  (
                    select
                      QD.study_id,
                      QD.country_id,
                      QD.countryName,
                      QD.site_id,
                      QD.siteName,
                      QD.milestone_id,
                      QD.nTotalCompleted,
                      QD.nTotalMissing,
                      QD.participant_id,
                      QD.participant_start_date,
                      QD.nTotalCompletedPrev1Week,
                      QD.nTotalMissingPrev1Week,
                      RM.display_name as milestone_name,
                      RM.sort_order,
                      EXTRACT(
                        MONTH
                        from
                          QD.participant_start_date
                      ) AS 'month',
                      EXTRACT(
                        year
                        from
                          QD.participant_start_date
                      ) AS year
                    from
                      (
                        select
                          study_id,
                          country_id,
                          country_name as countryName,
                          site_id,
                          site_name as siteName,
                          participant_id,
                          activity_name as activityName,
                          milestone_id,
                          SUM(n_total_completed) nTotalCompleted,
                          SUM(n_total_missing) nTotalMissing,
                          SUM(n_total_completed_prev1wk) nTotalCompletedPrev1Week,
                          SUM(n_total_missing_prev1wk) nTotalMissingPrev1Week,
                          participant_start_date
                        from
                          (
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  milestone_id,
                                  participant_id,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      PTS.milestone_id,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ? 
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ar.id ar_id,
                                          pts.milestone_id,
                                          'activity' as response_type,
                                          '' as visit_status,
                                          ar.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ? 
                                          ) pts
                                          left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                          and pts.task_instance_id = ar.task_instance_id
                                          and pts.study_version_id = ar.study_version_id
                                        where
                                          pts.task_type IN ('activity')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  milestone_id
                              ) T1
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  milestone_id,
                                  participant_id,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      PTS.milestone_id,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ? 
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        SELECT
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          srt.id ar_id,
                                          pts.milestone_id,
                                          'survey' as response_type,
                                          '' as visit_status,
                                          srt.completion_time_utc as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ? 
                                          ) pts
                                          left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                          and pts.task_instance_id = srt.task_instance_id
                                          and pts.study_version_id = srt.study_version_id
                                        where
                                          pts.task_type IN ('survey', 'epro')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  milestone_id
                              ) T2
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  milestone_id,
                                  participant_id,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      PTS.milestone_id,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ? 
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ppa.id ar_id,
                                          pts.milestone_id,
                                          'visits' as response_type,
                                          ppa.status as visit_status,
                                          ppa.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ? 
                                          ) pts
                                          left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                          and pts.task_instance_id = ppa.task_instanceuuid
                                          and pts.study_version_id = ppa.study_version_id
                                          and (ppa.visit_id IS NOT NULL)
                                        WHERE
                                          pts.task_type IN ('telehealth')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  milestone_id
                              ) T3
                          ) F1
                        where
                          participant_start_date is not null
                        group by
                          participant_id,
                          milestone_id
                      ) QD
                      left join (
                        select
                          milestone_id,
                          display_name,
                          sort_order
                        from
                          research.milestones
                      ) RM ON QD.milestone_id = RM.milestone_id
                    where
                      RM.milestone_id is not null
                    order by
                      RM.sort_order
                  ) QM
                group by
                  month,
                  year,
                  sort_order
              ) tt1
          ) t1
          INNER JOIN (
            select
              join_date,
              max(sort_order) sort_order
            from
              (
                select
                  DATE(CONCAT(year, '-', month, '-01')) as join_date,
                  milestone_name,
                  milestone_id,
                  QM.sort_order,
                  count(participant_id) as n_participant,
                  SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate,
                  SUM(nTotalCompletedPrev1Week) /(
                    SUM(nTotalCompletedPrev1Week) + SUM(nTotalMissingPrev1Week)
                  ) as participation_rate_prev1week
                from
                  (
                    select
                      QD.study_id,
                      QD.country_id,
                      QD.countryName,
                      QD.site_id,
                      QD.siteName,
                      QD.milestone_id,
                      QD.nTotalCompleted,
                      QD.nTotalMissing,
                      QD.participant_id,
                      QD.participant_start_date,
                      QD.nTotalCompletedPrev1Week,
                      QD.nTotalMissingPrev1Week,
                      RM.display_name as milestone_name,
                      RM.sort_order,
                      EXTRACT(
                        MONTH
                        from
                          QD.participant_start_date
                      ) AS 'month',
                      EXTRACT(
                        year
                        from
                          QD.participant_start_date
                      ) AS year
                    from
                      (
                        select
                          study_id,
                          country_id,
                          country_name as countryName,
                          site_id,
                          site_name as siteName,
                          participant_id,
                          activity_name as activityName,
                          milestone_id,
                          SUM(n_total_completed) nTotalCompleted,
                          SUM(n_total_missing) nTotalMissing,
                          SUM(n_total_completed_prev1wk) nTotalCompletedPrev1Week,
                          SUM(n_total_missing_prev1wk) nTotalMissingPrev1Week,
                          participant_start_date
                        from
                          (
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  milestone_id,
                                  participant_id,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      PTS.milestone_id,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ? 
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ar.id ar_id,
                                          pts.milestone_id,
                                          'activity' as response_type,
                                          '' as visit_status,
                                          ar.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ? 
                                          ) pts
                                          left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                          and pts.task_instance_id = ar.task_instance_id
                                          and pts.study_version_id = ar.study_version_id
                                        where
                                          pts.task_type IN ('activity')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  milestone_id
                              ) T1
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  milestone_id,
                                  participant_id,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      PTS.milestone_id,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ? 
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        SELECT
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          srt.id ar_id,
                                          pts.milestone_id,
                                          'survey' as response_type,
                                          '' as visit_status,
                                          srt.completion_time_utc as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ? 
                                          ) pts
                                          left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                          and pts.task_instance_id = srt.task_instance_id
                                          and pts.study_version_id = srt.study_version_id
                                        where
                                          pts.task_type IN ('survey', 'epro')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  milestone_id
                              ) T2
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  milestone_id,
                                  participant_id,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      PTS.milestone_id,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                  )
                                  AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                  AND PTS.end_day >= (
                                    TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                  ) THEN 'Scheduled' END AS status
                                from
                                  (
                                    select
                                      *
                                    from
                                      research_analytics.participant_site_country
                                    where
                                      study_id = ? 
                                      ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                      ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                  ) PSC
                                  INNER JOIN (
                                    select
                                      pts.participant_id,
                                      PTS.study_id,
                                      pts.task_title,
                                      pts.created_date,
                                      pts.start_day,
                                      pts.end_day,
                                      ppa.id ar_id,
                                      pts.milestone_id,
                                      'visits' as response_type,
                                      ppa.status as visit_status,
                                      ppa.end_time as completion_date
                                    FROM
                                      (
                                        select
                                          *
                                        from
                                          research_response.participant_task_schedule
                                        where
                                          study_id = ? 
                                      ) pts
                                      left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                      and pts.task_instance_id = ppa.task_instanceuuid
                                      and pts.study_version_id = ppa.study_version_id
                                      and (ppa.visit_id IS NOT NULL)
                                    WHERE
                                      pts.task_type IN ('telehealth')
                                  ) PTS ON PSC.participant_id = PTS.participant_id
                                  and PSC.study_id = PTS.study_id
                                ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                              ) t1
                            group by
                              participant_id,
                              milestone_id
                          ) T3
                      ) F1
                    where
                      participant_start_date is not null
                    group by
                      participant_id,
                      milestone_id
                  ) QD
                  left join (
                    select
                      milestone_id,
                      display_name,
                      sort_order
                    from
                      research.milestones
                  ) RM ON QD.milestone_id = RM.milestone_id
                where
                  RM.milestone_id is not null
                order by
                  RM.sort_order
              ) QM
            group by
              month,
              year,
              sort_order
          ) tt2
        where
          participation_rate is not null
        group by
          join_date
      ) t2 ON t1.join_date = t2.join_date
      and t1.sort_order = t2.sort_order
    
      `;

      console.log(`getRetentionScoreMilestone query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getRetentionScoreMilestone:', error);
      throw error;
    }
  }

  async getRetentionScoreCustom(params) {
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
    try {
      const bindingParams = []
      for (let index = 0; index < 14; index++) {
        bindingParams.push(params.studyId)
      }

      const querySql = `
        select
          AVG(t1.participation_rate) * 10 as retention_score,
          AVG(t1.participation_rate_prev1week) * 10 as retention_score_prev1week
        from
          (
            select
              join_date,
              sort_order,
              participation_rate,
              participation_rate_prev1week
            from
              (
                select
                  DATE(CONCAT(year, '-', month, '-01')) as join_date,
                  bucket_name,
                  QM.sort_order,
                  count(participant_id) as n_participant,
                  SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate,
                  SUM(nTotalCompletedPrev1Week) /(
                    SUM(nTotalCompletedPrev1Week) + SUM(nTotalMissingPrev1Week)
                  ) as participation_rate_prev1week
                from
                  (
                    select
                      QD.study_id,
                      QD.country_id,
                      QD.countryName,
                      QD.site_id,
                      QD.siteName,
                      QD.nTotalCompleted,
                      QD.nTotalMissing,
                      QD.participant_id,
                      QD.participant_start_date,
                      nTotalCompletedPrev1Week,
                      nTotalMissingPrev1Week,
                      QD.bucket_day bucket_name,
                      RM.sort_order,
                      EXTRACT(
                        MONTH
                        from
                          QD.participant_start_date
                      ) AS 'month',
                      EXTRACT(
                        year
                        from
                          QD.participant_start_date
                      ) AS year
                    from
                      (
                        select
                          study_id,
                          country_id,
                          country_name as countryName,
                          site_id,
                          participant_start_date,
                          site_name as siteName,
                          bucket_day,
                          SUM(n_total_completed) nTotalCompleted,
                          SUM(n_total_missing) nTotalMissing,
                          SUM(n_total_completed_prev1wk) nTotalCompletedPrev1Week,
                          SUM(n_total_missing_prev1wk) nTotalMissingPrev1Week,
                          participant_id
                        from
                          (
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  participant_id,
                                  bucket_day,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      CONCAT(
                                        'DAY ',
                                        FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                      ) as bucket_day,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ?
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ar.id ar_id,
                                          'activity' as response_type,
                                          '' as visit_status,
                                          ar.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ?
                                              and schedule_type = 'custom'
                                          ) pts
                                          left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                          and pts.task_instance_id = ar.task_instance_id
                                          and pts.study_version_id = ar.study_version_id
                                        where
                                          pts.task_type IN ('activity')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  bucket_day
                              ) T1
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  participant_id,
                                  bucket_day,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      CONCAT(
                                        'DAY ',
                                        FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                      ) as bucket_day,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ?
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        SELECT
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          srt.id ar_id,
                                          'survey' as response_type,
                                          '' as visit_status,
                                          srt.completion_time_utc as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ?
                                              and schedule_type = 'custom'
                                          ) pts
                                          left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                          and pts.task_instance_id = srt.task_instance_id
                                          and pts.study_version_id = srt.study_version_id
                                        where
                                          pts.task_type IN ('survey', 'epro')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  bucket_day
                              ) T2
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  participant_id,
                                  bucket_day,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      CONCAT(
                                        'DAY ',
                                        FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                      ) as bucket_day,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ?
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ppa.id ar_id,
                                          'visits' as response_type,
                                          ppa.status as visit_status,
                                          ppa.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ?
                                              and schedule_type = 'custom'
                                          ) pts
                                          left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                          and pts.task_instance_id = ppa.task_instanceuuid
                                          and pts.study_version_id = ppa.study_version_id
                                          and (ppa.visit_id IS NOT NULL)
                                        WHERE
                                          pts.task_type IN ('telehealth')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  bucket_day
                              ) T3
                          ) F1
                        where
                          participant_start_date is not null
                        group by
                          participant_id,
                          bucket_day
                      ) QD
                      left join (
                        select
                          DISTINCT CONCAT(
                            'DAY ',
                            FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                          ) as bucket_day,
                          FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                          FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                        from
                          (
                            Select
                              distinct start_day
                            from
                              research_response.participant_task_schedule
                            where
                              study_id = ?
                              and schedule_type = 'custom'
                          ) t1
                      ) RM ON QD.bucket_day = RM.bucket_day
                    order by
                      RM.sort_order
                  ) QM
                group by
                  month,
                  year,
                  sort_order
              ) tt1
          ) t1
          INNER JOIN (
            select
              join_date,
              max(sort_order) sort_order
            from
              (
                select
                  DATE(CONCAT(year, '-', month, '-01')) as join_date,
                  bucket_name,
                  QM.sort_order,
                  count(participant_id) as n_participant,
                  SUM(nTotalCompleted) /(SUM(nTotalCompleted) + SUM(nTotalMissing)) as participation_rate,
                  SUM(nTotalCompletedPrev1Week) /(
                    SUM(nTotalCompletedPrev1Week) + SUM(nTotalMissingPrev1Week)
                  ) as participation_rate_prev1week
                from
                  (
                    select
                      QD.study_id,
                      QD.country_id,
                      QD.countryName,
                      QD.site_id,
                      QD.siteName,
                      QD.nTotalCompleted,
                      QD.nTotalMissing,
                      QD.participant_id,
                      QD.participant_start_date,
                      nTotalCompletedPrev1Week,
                      nTotalMissingPrev1Week,
                      QD.bucket_day bucket_name,
                      RM.sort_order,
                      EXTRACT(
                        MONTH
                        from
                          QD.participant_start_date
                      ) AS 'month',
                      EXTRACT(
                        year
                        from
                          QD.participant_start_date
                      ) AS year
                    from
                      (
                        select
                          study_id,
                          country_id,
                          country_name as countryName,
                          site_id,
                          participant_start_date,
                          site_name as siteName,
                          bucket_day,
                          SUM(n_total_completed) nTotalCompleted,
                          SUM(n_total_missing) nTotalMissing,
                          SUM(n_total_completed_prev1wk) nTotalCompletedPrev1Week,
                          SUM(n_total_missing_prev1wk) nTotalMissingPrev1Week,
                          participant_id
                        from
                          (
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  participant_id,
                                  bucket_day,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      CONCAT(
                                        'DAY ',
                                        FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                      ) as bucket_day,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ?
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ar.id ar_id,
                                          'activity' as response_type,
                                          '' as visit_status,
                                          ar.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ?
                                              and schedule_type = 'custom'
                                          ) pts
                                          left join research_response.activity_response ar on pts.participant_id = ar.participant_id
                                          and pts.task_instance_id = ar.task_instance_id
                                          and pts.study_version_id = ar.study_version_id
                                        where
                                          pts.task_type IN ('activity')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  bucket_day
                              ) T1
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  participant_id,
                                  bucket_day,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      CONCAT(
                                        'DAY ',
                                        FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                      ) as bucket_day,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ?
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        SELECT
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          srt.id ar_id,
                                          'survey' as response_type,
                                          '' as visit_status,
                                          srt.completion_time_utc as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ?
                                              and schedule_type = 'custom'
                                          ) pts
                                          left join research_response.survey_response_tracker srt on pts.participant_id = srt.participant_id
                                          and pts.task_instance_id = srt.task_instance_id
                                          and pts.study_version_id = srt.study_version_id
                                        where
                                          pts.task_type IN ('survey', 'epro')
                                      ) PTS ON PSC.participant_id = PTS.participant_id
                                      and PSC.study_id = PTS.study_id
                                    ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                  ) t1
                                group by
                                  participant_id,
                                  bucket_day
                              ) T2
                            UNION
                            select
                              *
                            from
                              (
                                select
                                  study_id,
                                  country_id,
                                  country_name,
                                  site_id,
                                  participant_start_date,
                                  site_name,
                                  activity_name,
                                  participant_id,
                                  bucket_day,
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_completed',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(curdate(), interval 1 day)
                                    )
                                    ${params.fromDate ? ` and (task_date between '${params.fromDate}' and '${params.toDate}')` : ''}
                                    then 1 end
                                  ) as 'n_total_missing',
                                  count(
                                    case when (status = 'Complete')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_completed_prev1wk',
                                  count(
                                    case when (status = 'Missed')
                                    and (
                                      task_date <= DATE_ADD(
                                        DATE_ADD(curdate(), interval 1 day),
                                        interval -7 day
                                      )
                                    )
                                    ${params.fromDate ? ` and (task_date between DATE_ADD('${params.fromDate}', interval -7 day) and DATE_ADD('${params.toDate}', interval -7 day))` : ''}  
                                    then 1 end
                                  ) as 'n_total_missing_prev1wk'
                                from
                                  (
                                    select
                                      PSC.participant_id,
                                      PSC.site_name,
                                      PSC.site_id,
                                      participant_start_date,
                                      PSC.study_name,
                                      PSC.study_id,
                                      PSC.country_id,
                                      PSC.country_name,
                                      PTS.task_title as activity_name,
                                      CONCAT(
                                        'DAY ',
                                        FLOOR(((FLOOR((PTS.start_day) / 30) + 1) * 30))
                                      ) as bucket_day,
                                      PSC.enrollment_date as task_date,
                                      CASE WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status != 'Reschedule' THEN 'Complete' WHEN PTS.response_type = 'visits'
                                      AND PTS.ar_id IS NOT NULL
                                      AND PTS.visit_status = 'Reschedule' THEN PTS.visit_status WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND end_day - start_day >= 1 THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type = 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' WHEN PTS.response_type != 'visits'
                                      AND PTS.ar_id IS NOT NULL THEN 'Complete' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day = (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.end_day - PTS.start_day >= 1 THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day < (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      )
                                      AND PTS.ar_id IS NULL THEN 'Missed' WHEN PTS.response_type != 'visits'
                                      AND PTS.end_day >= (
                                        TIMESTAMPDIFF(day, PSC.participant_start_date, NOW())
                                      ) THEN 'Scheduled' END AS status
                                    from
                                      (
                                        select
                                          *
                                        from
                                          research_analytics.participant_site_country
                                        where
                                          study_id = ?
                                          ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                                          ${params.participantsIds ? `and participant_id in (${participantsIds}) ` : ''}
                                      ) PSC
                                      INNER JOIN (
                                        select
                                          pts.participant_id,
                                          PTS.study_id,
                                          pts.task_title,
                                          pts.created_date,
                                          pts.start_day,
                                          pts.end_day,
                                          ppa.id ar_id,
                                          'visits' as response_type,
                                          ppa.status as visit_status,
                                          ppa.end_time as completion_date
                                        FROM
                                          (
                                            select
                                              *
                                            from
                                              research_response.participant_task_schedule
                                            where
                                              study_id = ?
                                              and schedule_type = 'custom'
                                          ) pts
                                          left join research_response.pi_participant_appointment ppa on pts.participant_id = ppa.participant_id
                                          and pts.task_instance_id = ppa.task_instanceuuid
                                          and pts.study_version_id = ppa.study_version_id
                                          and (ppa.visit_id IS NOT NULL)
                                        WHERE
                                        pts.task_type IN ('telehealth')
                                    ) PTS ON PSC.participant_id = PTS.participant_id
                                    and PSC.study_id = PTS.study_id
                                  ${params.fromDate ? ` WHERE PSC.enrollment_date BETWEEN DATE_ADD('${params.fromDate}', interval -7 day) and '${params.toDate}' ` : ''}
                                ) t1
                              group by
                                participant_id,
                                bucket_day
                            ) T3
                        ) F1
                      where
                        participant_start_date is not null
                      group by
                        participant_id,
                        bucket_day
                    ) QD
                    left join (
                      select
                        DISTINCT CONCAT(
                          'DAY ',
                          FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30))
                        ) as bucket_day,
                        FLOOR(((FLOOR((t1.start_day) / 30) + 1))) sort_order,
                        FLOOR(((FLOOR((t1.start_day) / 30) + 1) * 30)) as start_day
                      from
                        (
                          Select
                            distinct start_day
                          from
                            research_response.participant_task_schedule
                          where
                            study_id = ?
                            and schedule_type = 'custom'
                        ) t1
                    ) RM ON QD.bucket_day = RM.bucket_day
                  order by
                    RM.sort_order
                ) QM
              group by
                month,
                year,
                sort_order
            ) tt2
          where
            participation_rate is not null
          group by
            join_date
        ) t2 ON t1.join_date = t2.join_date
        and t1.sort_order = t2.sort_order
      
      `;

      console.log(`getRetentionScoreCustom query SQL ${JSON.stringify(params)} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getRetentionScoreCustom:', error);
      throw error;
    }
  }

  async getStudyProgressionData(params) {
    const dbConnection = await this._initDbConnectionPool(
      this.clientId,
      RESEARCH_ANALYTICS_DB
    );
    try {
      const bindingParams = []
      bindingParams.push(params.studyId)

      if (params.participantsIds) {
        params.participantsIds = convertArrayToString(params.participantsIds);
      }
      let query = `select  * from (
        select 
          coalesce(
            psc.participant_name, psc.participant_id
          ) as participant_id, 
          psc.country_name, 
          psc.site_name, 
          (
            case when (
              coalesce(
                ${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` }, 
                current_date()
              ) <= date(
                DATE_ADD(
                  date(psc.active_date), 
                  INTERVAL datediff(
                    smd.first_patient_last_date, smd.first_patient_enroll_date
                  ) DAY
                ))
            ) THEN datediff(
              coalesce(
                ${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` }, 
                current_date()
              ), 
              date(psc.active_date)
            ) ELSE datediff(
                date(
                  DATE_ADD(
                    date(psc.active_date), 
                    INTERVAL datediff(
                      smd.first_patient_last_date, smd.first_patient_enroll_date
                    ) DAY
                  )), 
              date(psc.active_date)
            ) END
          ) as days_in_study 
        from 
          research_analytics.participant_site_country psc 
          left join research.study_meta_data smd on smd.id = psc.study_id 
          left join (
            select case when(
                coalesce(
                  ${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` }, 
                  curdate()
                ) < date(max(modified_time))
              ) then coalesce(
                ${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` }, 
                curdate()
              ) when(
                coalesce(
                  ${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` }, 
                  curdate()
                ) >= date(max(modified_time))
              ) then date(
                max(modified_time)
              ) end as discontinue_date, 
              participant_id 
            from 
              research.participant_status_history psh 
            where 
              new_status in(
                'DISQUALIFIED', 'WITHDRAWSTUDY', 'DISCONTINUED'
              ) 
            group by 
              participant_id
          ) ACT on psc.participant_id = ACT.participant_id 
        where 
          psc.study_id = ? 
          and psc.active_date <> '' 
          ${params.siteId ? `and psc.site_id =  '${params.siteId}'` : ''}
          ${params.participantsIds ? `and psc.participant_id IN (${params.participantsIds}) ` : ''}
          and coalesce(
            ${params.fromDate && params.toDate ? `'${params.toDate}' ` : `NULL` }, 
            curdate()
          ) <= coalesce(
            discontinue_date, 
            curdate()
          ) 
        group by participant_id
      ) a 
      where days_in_study >= 1`;
      utils.createLog('', `getStudyProgression query`, query);
      const [data] = await dbConnection.query(query, bindingParams)
      utils.createLog('', `getStudyProgression Data`, data);
      return data;
    } catch (error) {
      utils.createLog('', `Error in function getStudyProgressionData:`, error);
      throw error;
    } finally {
      dbConnection.end();
      utils.createLog('', `Connection closed in finally`);
    }
  }
  
}

module.exports = RetentionModel;