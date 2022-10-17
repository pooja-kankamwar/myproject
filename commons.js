const BaseModel = require("./baseModel");
const utils = require('../config/utils');
const {constants: {DATABASE: {RESEARCH_DB}}} = require('../constants')


/**
 * Class representing a message model.
 * @class
 */
class CommonsModel extends BaseModel {
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

    async getAllUsersData(studyId, siteId){
        const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_DB);
        try {
          const bindingParams = []
          bindingParams.push(studyId)
          let querySql = `
            select id, COALESCE(invitation_date,registration_date) as enrollmentDate, site_id as siteId, user_defined_participant_id as userDefinedId, patient_id as patientId
          
            from research.participant
            where study_id = ?
          `
          if (siteId) {
              bindingParams.push(siteId)
              querySql += ' AND site_id = ? '
          }

          const [data] = await dbConnectionPool.query(querySql, bindingParams)
          dbConnectionPool.end();
          return data;
        } catch (error) {
          dbConnectionPool.end();
          console.log('Error in function getAllUsersData:', error);
          throw error;
        }
    }

    async getStudyMetaData(params){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_DB);
      try {
        const bindingParams = [];
        let querySql = `
        select coalesce(study_start_date, created_time) as fromDate, coalesce(study_end_date, curdate()) as toDate 
        from research.study_meta_data where id = ${this.setBindingParams(bindingParams, params.studyId)}
        `
        console.log(`getStudyMetaData query SQL for id ${params.studyId} : ${JSON.stringify(params)} \n${querySql}`);
        console.log(`getStudyMetaData binding params ${params.studyId} \n${JSON.stringify(bindingParams)}`);
        console.time(`getStudyMetaData query SQL time ${params.studyId} Ends in:`)
        const [data] = await dbConnectionPool.query(querySql, bindingParams);
        console.timeEnd(`getStudyMetaData query SQL time ${params.studyId} Ends in:`)
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function getAllUsersData:', error);
        throw error;
      }
  }

    async initiateStudyTaskData(studyId){
      const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_DB);
      try {
        const bindingParams = []
        bindingParams.push(studyId);
        let deleteSql = `delete from research_analytics.analytics_task_schedule where study_id = ?`;
        console.log(`initiateStudyTaskData Delete query SQL ${studyId} \n${deleteSql}`);
        const [deleteData] = await dbConnectionPool.query(deleteSql, bindingParams);
        let querySql = `
        INSERT INTO research_analytics.analytics_task_schedule
        select
        QA.id,
        QA.participant_id,
        QA.study_version_id,
        QA.task_instance_id,
        QA.task_id,
        QA.task_type,
        QA.task_title,
        QA.task_class_name,
        QA.task_file_name,
        QA.task_start_date,
        QA.task_end_date,
        schedule_id,
        scheduled_date,
        allow_multiple,
        sequence,
        QA.study_id,
        start_day,
        end_day,
        schedule_day,
        schedule_count,
        created_date,
        schedule_type,
        milestone_id,
        is_watch_enable,
        COALESCE(ppa.device_id, ar.device_id, srt.device_id) as device_id,
        QA.enabled,
        CASE
        WHEN QA.task_type = 'activity'
          THEN ar.start_time
        WHEN QA.task_type in ('survey', 'epro') THEN null
        WHEN QA.task_type in ('telehealth')
          THEN ppa.start_time
        ELSE null
        END
            start_time_utc,
        CASE
          WHEN QA.task_type = 'activity'
              THEN ar.end_time
          WHEN QA.task_type in ('survey', 'epro') THEN srt.completion_time_utc
          WHEN QA.task_type in ('telehealth')
              THEN ppa.end_time
          ELSE null
          END
                end_time_utc,
          COALESCE(ar.participant_local_start_time, ppa.appointement_time_local, srt.completion_time_local) as start_time_local,
          CASE
          WHEN QA.task_type = 'activity'
              THEN ar.participant_local_end_time
          WHEN QA.task_type in ('survey', 'epro') THEN srt.completion_time_local
          WHEN QA.task_type in ('telehealth')
              THEN appointement_time_local
          ELSE null
          END
                end_time_local,
          CASE
          WHEN QA.task_type = 'activity'
              THEN ar.participant_local_time_offset
          WHEN QA.task_type in ('survey', 'epro') THEN null
          WHEN QA.task_type in ('telehealth')
              THEN appointement_time_offset
          ELSE null
          END
                time_offset,
        CASE
          WHEN QA.task_type = 'activity'
              THEN ar.status
          WHEN QA.task_type in ('survey', 'epro') and
                srt.completion_time_local is not null THEN 'Completed'
          WHEN QA.task_type in ('telehealth')
              THEN ppa.status
          ELSE null
          END
                status,
        COALESCE(ppa.activity_scheduling_mode, ar.activity_scheduling_mode, QA.activity_scheduling_mode) as activity_scheduling_mode,
        ar.activity_name,
        ar.time_taken,
        ar.activity_key,
        srt.revision_id,
        srt.revision_no,
        ppa.pi_id,
        ppa.visit_id,
        ppa.appointment_date,
        visit_type,
        ppa.visit_name,
        ppa.appointement_time_utc,
        ppa.appointement_time_local,
        ppa.appointement_time_offset,
        ppa.pi_availability_id,
        ppa.pi_availability_slot
        from ( select *
                                from  research_response.participant_task_schedule pts where study_id = ?
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
        `
        console.time(`initiateStudyTaskData for ${studyId} Done in`);
        console.log(`initiateStudyTaskData query SQL ${studyId} \n${querySql}`);
        const [data] = await dbConnectionPool.query(querySql, bindingParams)
        console.timeEnd(`initiateStudyTaskData for ${studyId} Done in`);
        dbConnectionPool.end();
        return data;
      } catch (error) {
        dbConnectionPool.end();
        console.log('Error in function initiateStudyTaskData:', error);
        throw error;
      }
  }
  async getbenchmarkData(studyId,transferType){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_DB);
    try {
      const bindingParams = []
      bindingParams.push(studyId)
      let querySql = `select id, study_id as studyId, transfer_type as transferType, target_url as targetUrl, startDate , endDate from research.dt_config where study_id = ? AND transfer_type= ? And NOW() BETWEEN startDate AND endDate`
      if (transferType) {
          bindingParams.push(transferType);
      }

      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      dbConnectionPool.end();
      return data;
    } catch (error) {
      dbConnectionPool.end();
      console.log('Error in function getbenchmarkData:', error);
      throw error;
    }
}

  async getMilestones(studyId){
    const dbConnectionPool = await this._initDbConnectionPool(this.clientId, RESEARCH_DB);
    try {
      const bindingParams = []
      bindingParams.push(studyId)
      let querySql = `select smd.id,
        m.milestone_name,
        m.milestone_day,
        DATEDIFF(smd.first_patient_last_date, smd.first_patient_enroll_date ) as study_length
        from   research.study_meta_data smd
        left join   research.milestones m
        on m.study_id = smd.id
        where id= ?
        ORDER BY m.milestone_day ASC`;
      utils.createLog('', `getMilestone query SQL ${studyId} \n${querySql}`);
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      utils.createLog('' , `getMilestone Data`, data);
      return data;
    } catch (error) {
      utils.createLog('', 'Error in function getMilestone:', error);
      throw error;
    } finally {
      utils.createLog('', `Connection closed in finally`);
      dbConnectionPool.end();
    }
  }
}

module.exports = CommonsModel;
