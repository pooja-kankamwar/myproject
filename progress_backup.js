
async getParticipantProgress (params) {
  const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_ANALYTICS_DB);
  try {
    const bindingParams = [params.studyId]
    let participantIds = null;
    let offset = (params.page-1)*params.limit
    if (params.participantIds) {
      participantIds = convertArrayToString(params.participantIds);
    }

    let querySql = `
    select
    QA.study_id,
    IFNULL(
      QA.country_name, 'UnknownCountry'
    ) AS countryName,
    IFNULL(QA.site_name, 'UnknownSite') AS siteName,
    QA.participant_id as participantId,
    IFNULL(
      QA.task_title, 'UnknownActivity'
    ) AS activityTitle,
    IFNULL(QA.sequence, 0) AS activityOrder,
    CASE when QA.n_complete > QA.task_frequency then QA.task_frequency when QA.n_complete is null then 0 else QA.n_complete end as nComplete,
    IFNULL(QA.task_frequency, 0) as taskFrequency,
    CASE when QA.n_complete >= QA.task_frequency
    and n_complete != 0
    and (
      (
        TIMESTAMPDIFF(
          day, QA.participant_start_date,
          NOW()
        )
      ) - QA.start_day
    ) > 0 then 'Completed' when NOW() BETWEEN DATE_ADD(
      QA.participant_start_date, INTERVAL QA.start_day DAY
    )
    and DATE_ADD(
      QA.participant_start_date,
      INTERVAL (QA.end_day - 1) DAY
    ) then 'Ongoing' when NOW() < DATE_ADD(
      QA.participant_start_date, INTERVAL QA.start_day DAY
    ) then 'Upcoming' else 'NotComplete' end as status,
    IFNULL(
      QA.participant_name, 'UnknownName'
    ) AS participantName,
    QA.participant_start_date as participantEnrollmentDate,
    QA.start_day as scheduledDays from (
        
          select QA.study_id,
                  QA.participant_id,
                  QA.participant_name,
                  QA.country_name,
                  QA.site_name,
                  QA.participant_start_date,
                  QA.task_title,
                  QA.sequence,
                  count(*)       task_frequency,
                  max(end_day)   end_day,
                  min(start_day) start_day,
                  COUNT(
                          CASE
                              WHEN QA.task_type = 'activity' and ar.status = 'Completed'
                                  THEN 1
                              WHEN QA.task_type in ('survey', 'epro') and
                                  srt.completion_time_local is not null THEN 1
                              WHEN QA.task_type in ('telehealth') and ppa.status = 'Complete'
                                  THEN 1
                              END
                      )          n_complete
          from (
                    select psc.participant_id,
                          psc.country_name,
                          psc.site_name,
                          psc.participant_start_date,
                          psc.participant_name,
                          task_instance_id,
                          task_id,
                          task_title,
                          schedule_type,
                          pts.study_version_id,
                          psc.study_id,
                          sequence,
                          start_day,
                          end_day,
                          task_type
                    from (select *
                          from research_analytics.participant_site_country
                          where study_id = ?
                            and participant_start_date is not null
                            ${params.participantIds ? ` and participant_id IN (${participantIds}) ` : ''} 
                            ${params.siteId ? `and site_id = '${params.siteId}' ` : ''}
                            ${params.fromDate ? `and participant_start_date BETWEEN '${params.fromDate}' AND '${params.toDate}'` : ''}
                          order by participant_id
                          LIMIT ${offset},${params.limit} ) psc
                            inner join research_response.participant_task_schedule pts
                                        on pts.study_id = psc.study_id and pts.participant_id = psc.participant_id
                ) QA
                    LEFT JOIN research_response.activity_response ar
                              on ar.study_id = QA.study_id
                                  and ar.participant_id = QA.participant_id
                                  and ar.task_instance_id = QA.task_instance_id
                                  and ar.study_version_id = QA.study_version_id
                    LEFT JOIN research_response.survey_response_tracker srt
                              on QA.study_id = srt.study_id
                                  and srt.participant_id = QA.participant_id
                                  and srt.task_instance_id = QA.task_instance_id
                                  and srt.study_version_id = QA.study_version_id
                    LEFT JOIN research_response.pi_participant_appointment ppa
                              on QA.study_id = ppa.study_id
                                  and ppa.participant_id = QA.participant_id
                                  and ppa.task_instanceuuid = QA.task_instance_id
                                  and ppa.study_version_id = QA.study_version_id
                                  and (ppa.visit_id IS NOT NULL)
          GROUP BY QA.participant_id, QA.task_id
      ) QA
  ORDER BY
    countryName ASC,
    siteName ASC,
    participantId ASC,
    QA.start_day ASC,
    activityTitle ASC;
    `
    console.log(`getParticipantProgress query SQL ${JSON.stringify(params)} \n${querySql}`);
    console.time(`getParticipantProgress query SQL ${JSON.stringify(params)} Ends in:`)
    const [data] = await dbConnectionPool.query(querySql, bindingParams)
    console.timeEnd(`getParticipantProgress query SQL ${JSON.stringify(params)} Ends in:`)
    dbConnectionPool.end()
    return data
  } catch (er) {
    dbConnectionPool.end()
    console.log('[Error in function getParticipantProgress]', er)
    throw er
  }
}