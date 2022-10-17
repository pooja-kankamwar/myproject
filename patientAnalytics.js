const BaseModel = require("./baseModel");
const CONST_VAL = require('../constants');
const { convertArrayToString } = require('../utils')

/**
 * Class representing a message model.
 * @class
 */
class PatientAnalyticsModel extends BaseModel {
  /**
   * Constructor.
   *
   * @param  {Object}  opts
   */
  constructor(opts) {
    super(opts);
    this.table = "enrollment";
    this._hasTimestamps = false;
    this.clientId = opts.clientId;
  }

  async getParticipantInsight (params) {
    let dbConnectionPool = null;
    if (params.forcePPMDStudy) {
      dbConnectionPool = await this._initDbPPMDConnectionPool(this.clientId, CONST_VAL.constants.DATABASE.RESEARCH_ANALYTICS_DB);
    } else {
      dbConnectionPool = await this._initDbConnectionPool(this.clientId, CONST_VAL.constants.DATABASE.RESEARCH_ANALYTICS_DB);
    }
    try {
      let bindingParams = [];
      let questionIds = null;
      if (params.questionIds) {
        questionIds = convertArrayToString(params.questionIds);
      }
      let querySql = `
      SELECT 
      surveyQuestion, 
      questionId, 
      questionType, 
      surveyTitle, 
      choosenAnswer, 
      milestoneDay as milestoneName, 
      case when count(case when flag_me = 'Yes' then 1 end) > 0 then 'Yes' else 'No' end as flag_me, 
      choosenAnswer / COUNT(
        distinct (participant_id)
      ) as avg_answer, 
      COUNT(
        distinct (participant_id)
      ) as total 
    FROM 
      (
        SELECT 
          q1.participant_id, 
          q1.surveyQuestion, 
          q1.questionId, 
          q1.questionType, 
          q1.surveyTitle, 
          q1.choosenAnswer, 
          q1.milestoneName, 
          q1.milestoneOrder, 
          q1.milestoneDay, 
          CASE WHEN q1.questionId = q2.question_id 
          and q1.choosenAnswer = q2.answer_text THEN 'Yes' ELSE 'No' END AS flag_me 
        from 
          (
            SELECT 
              a.participant_id, 
              a.question_text as surveyQuestion, 
              a.question_id as questionId, 
              a.question_type as questionType, 
              b.task_title as surveyTitle, 
              CASE WHEN a.question_type in (
                'MultipleChoice', 'MultipleChoiceImage', 
                'MultiChoice'
              ) THEN a.answer_text ELSE a.real_answer_text END AS choosenAnswer, 
              concat('Milestone ', b.sequence + 1) as milestoneName, 
              b.sequence as milestoneOrder, 
              b.start_day as milestoneDay 
            FROM 
              (
                SELECT 
                  MSR.id, 
                  MSR.study_id, 
                  MSR.participant_id, 
                  MSR.question_id, 
                  MSR.question_text, 
                  MSR.question_type, 
                  MSR.survey_id, 
                  MSR.survey_response_tracker_id, 
                  COALESCE(MSR.answer_text, '') as real_answer_text, 
                  case when locate(', ', MSR.answer_text) then substring_index(
                    substring_index(MSR.answer_text, ', ', numbers.rn), 
                    ', ', 
                    -1
                  ) 
                  when MSR.answer_text is NULL then ''
                  else substring_index(
                    substring_index(MSR.answer_text, ',', numbers.rn), 
                    ',', 
                    -1
                  ) end as answer_text, 
                  MSR.end_time 
                FROM 
                  (
                    select 
                      @num := @num + 1 as rn 
                    FROM 
                      research_response.survey_response a, 
                      (
                        select 
                          @num := 0
                      ) b 
                    limit 
                      100
                  ) numbers 
                  inner join research_response.survey_response MSR on char_length(COALESCE(MSR.answer_text,'')) - char_length(
                    replace(COALESCE(MSR.answer_text, ''),',', '')
                  ) >= numbers.rn - 1
                where 
                  MSR.study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                  and MSR.question_id in (${this.setBindingParams(bindingParams, params.questionIds)})
                  and MSR.question_text is not null 
                  and MSR.question_text != '' 
                  and MSR.question_id is not null 
                  and MSR.question_id != ''
                  and MSR.survey_response_tracker_id IN (
                    SELECT main.id FROM research_response.survey_response_tracker main 
                    where 
                      main.study_id= MSR.study_id 
                      and main.survey_id = MSR.survey_id 
                      and main.participant_id = MSR.participant_id
                      and main.completion_time_utc = (select max(completion_time_utc) from research_response.survey_response_tracker where survey_id = main.survey_id and participant_id = main.participant_id)
                    group by main.survey_id, main.participant_id
                  )
              ) a               
              left join (
                select 
                  task_id, 
                  task_title, 
                  study_id, 
                  participant_id, 
                  sequence, 
                  start_day 
                from 
                  (
                    select 
                      task_id, 
                      task_title, 
                      study_id, 
                      participant_id, 
                      sequence, 
                      min(start_day) as start_day 
                    from 
                      research_response.participant_task_schedule 
                    where 
                      study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                    group by 
                      task_id, 
                      task_title, 
                      study_id, 
                      participant_id, 
                      sequence 
                    union all 
                    select 
                      task_id, 
                      task_title, 
                      study_id, 
                      participant_id, 
                      sequence, 
                      min(start_day) as start_day 
                    from 
                      research_response.task_schedules_sync 
                    where 
                      study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                    group by 
                      task_id, 
                      task_title, 
                      study_id, 
                      participant_id, 
                      sequence
                  ) b1 
                where 
                  study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                group by 
                  task_id, 
                  task_title, 
                  study_id, 
                  participant_id, 
                  sequence
              ) b on a.survey_id = b.task_id 
              and a.study_id = b.study_id 
              and a.participant_id = b.participant_id
          ) q1 
          left join (
            select 
                  MSR.participant_id, 
                  MSR.question_id, 
                  MSR.question_text, 
                  COALESCE(MSR.answer_text,'') as answer_text, 
                  MSR.survey_id, 
                  MSR.study_id 
                from 
                  research_response.survey_response MSR
                where 
                  MSR.study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                  and MSR.participant_id = ${this.setBindingParams(bindingParams, params.participantId) } 
                  and MSR.question_id in (${this.setBindingParams(bindingParams, params.questionIds) })
                  and MSR.question_text is not null 
                  and MSR.question_text != '' 
                  and MSR.question_id is not null 
                  and MSR.question_id != ''
                  and MSR.survey_response_tracker_id IN (
                    SELECT main.id FROM research_response.survey_response_tracker main 
                    where 
                      main.study_id= MSR.study_id 
                      and main.survey_id = MSR.survey_id 
                      and main.participant_id = MSR.participant_id
                      and main.completion_time_utc = (select max(completion_time_utc) from research_response.survey_response_tracker where survey_id = main.survey_id and participant_id = main.participant_id)
                    group by main.survey_id, main.participant_id
                    )
          ) q2 on q1.questionId = q2.question_id 
          and q1.participant_id = q2.participant_id 
          and q1.choosenAnswer = q2.answer_text cross 
          join (
            select 
              participant_id, 
              max(sequence) as maxmilestoneOrder 
            from 
              (
                select 
                  a.participant_id, 
                  a.question_id, 
                  a.question_text, 
                  a.answer_text, 
                  a.survey_id, 
                  b.task_title, 
                  b.sequence, 
                  b.start_day 
                from 
                  (
                    select 
                      MSR.participant_id, 
                      MSR.question_id, 
                      MSR.question_text, 
                      COALESCE(MSR.answer_text,'') as answer_text, 
                      MSR.survey_id, 
                      MSR.study_id 
                    from 
                     research_response.survey_response MSR
                    where 
                      MSR.study_id = ${this.setBindingParams(bindingParams, params.studyId) }
                      and MSR.question_id in (${this.setBindingParams(bindingParams, params.questionIds) }) 
                      and MSR.question_text is not null 
                      and MSR.question_text != '' 
                      and MSR.question_id is not null 
                      and MSR.question_id != ''
                      and MSR.survey_response_tracker_id IN (
                        SELECT main.id FROM research_response.survey_response_tracker main 
                        where 
                          main.study_id= MSR.study_id 
                          and main.survey_id = MSR.survey_id 
                          and main.participant_id = MSR.participant_id
                          and main.completion_time_utc = (select max(completion_time_utc) from research_response.survey_response_tracker where survey_id = main.survey_id and participant_id = main.participant_id)
                        group by main.survey_id, main.participant_id
                      )
                  ) a 
                  left join (
                    select 
                      task_id, 
                      task_title, 
                      study_id, 
                      participant_id, 
                      sequence, 
                      start_day 
                    from 
                      (
                        select 
                          task_id, 
                          task_title, 
                          study_id, 
                          participant_id, 
                          sequence, 
                          min(start_day) as start_day 
                        from 
                          research_response.participant_task_schedule 
                        where 
                          study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                        group by 
                          task_id, 
                          task_title, 
                          study_id, 
                          participant_id, 
                          sequence 
                        union all 
                        select 
                          task_id, 
                          task_title, 
                          study_id, 
                          participant_id, 
                          sequence, 
                          min(start_day) as start_day 
                        from 
                          research_response.task_schedules_sync 
                        where 
                          study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                        group by 
                          task_id, 
                          task_title, 
                          study_id, 
                          participant_id, 
                          sequence
                      ) b1 
                    where 
                      study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                    group by 
                      task_id, 
                      task_title, 
                      study_id, 
                      participant_id, 
                      sequence
                  ) b on a.survey_id = b.task_id 
                  and a.study_id = b.study_id 
                  and a.participant_id = b.participant_id
              ) q3
          ) q4 
        where 
          q1.milestoneOrder <= q4.maxmilestoneOrder
      ) T1 
    group by 
      surveyQuestion, 
      questionId, 
      choosenAnswer, 
      milestoneDay
    order by 
      questionId, 
      milestoneDay asc
      `
      console.log(`getParticipantInsight query SQL ${params.studyId} \n${querySql}`);
      console.log(`getParticipantInsight params: ${JSON.stringify(params)}`);
      console.log(`getParticipantInsight binding_params: \n${JSON.stringify(bindingParams)}`);
      console.time(`getParticipantInsight query SQL ${params.studyId} Ends in:`)
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      console.timeEnd(`getParticipantInsight query SQL ${params.studyId} Ends in:`);
      dbConnectionPool.end()
      return data
    } catch (er) {
      dbConnectionPool.end()
      console.log('[Error in function getParticipantInsight]', er)
      throw er
    }
  }
  async getParticipantInsightVerticalBarchart(params){
    let dbConnectionPool = null;
    if (params.forcePPMDStudy) {
      dbConnectionPool = await this._initDbPPMDConnectionPool(this.clientId, CONST_VAL.constants.DATABASE.RESEARCH_ANALYTICS_DB);
    } else {
      dbConnectionPool = await this._initDbConnectionPool(this.clientId, CONST_VAL.constants.DATABASE.RESEARCH_ANALYTICS_DB);
    }
    try {
      let bindingParams = [];
      let questionIds = null;
      if (params.verticalBarchartQuestionIds) {
        questionIds = convertArrayToString(params.verticalBarchartQuestionIds);
      }
      let querySql = `
      SELECT 
  surveyQuestion, 
  questionId, 
  questionType, 
  surveyTitle, 
  choosenAnswer, 
  milestoneDay as milestoneName, 
  case when count(case when flag_me = 'Yes' then 1 end) > 0 then 'Yes' else 'No' end as flag_me, 
  choosenAnswer / COUNT(
    distinct (participant_id)
  ) as avg_answer, 
  COUNT(
    distinct (participant_id)
  ) as total 
FROM 
  (
    SELECT 
      q1.participant_id, 
      q1.surveyQuestion, 
      q1.questionId, 
      q1.questionType, 
      q1.surveyTitle, 
      q1.choosenAnswer, 
      q1.milestoneOrder, 
      q1.milestoneDay, 
      CASE WHEN q1.questionId = q2.question_id 
      and q1.choosenAnswer = q2.answer_text THEN 'Yes' ELSE 'No' END AS flag_me 
    from 
      (
        SELECT 
          distinct a.participant_id, 
          a.question_text as surveyQuestion, 
          a.question_id as questionId, 
          a.question_type as questionType, 
          b.task_title as surveyTitle, 
          CASE WHEN a.question_type in ('Integer') THEN a.answer_text ELSE a.real_answer_text END AS choosenAnswer, 
          b.sequence as milestoneOrder, 
          b.start_day as milestoneDay 
        FROM 
          (
            select 
              t1.id, 
              t1.study_id, 
              t1.participant_id, 
              t1.question_id, 
              t1.question_text, 
              t1.question_type, 
              t1.survey_response_tracker_id, 
              t1.answer_text, 
              t1.end_time, 
              t1.real_answer_text, 
              t2.task_instance_id, 
              t1.survey_id 
            from 
              (
                SELECT 
                  MSR.id, 
                  MSR.study_id, 
                  MSR.participant_id, 
                  MSR.question_id, 
                  MSR.question_text, 
                  MSR.question_type, 
                  MSR.survey_id, 
                  MSR.survey_response_tracker_id, 
                  COALESCE(MSR.answer_text, '') as real_answer_text, 
                  case when locate(', ', MSR.answer_text) then substring_index(
                    substring_index(
                      MSR.answer_text, ', ', numbers.rn
                    ), 
                    ', ', 
                    -1
                  ) when MSR.answer_text is NULL then '' else substring_index(
                    substring_index(MSR.answer_text, ',', numbers.rn), 
                    ',', 
                    -1
                  ) end as answer_text, 
                  MSR.end_time 
                FROM 
                  (
                    select 
                      @num := @num + 1 as rn 
                    FROM 
                      research_response.survey_response a, 
                      (
                        select 
                          @num := 0
                      ) b 
                    limit 
                      100
                  ) numbers 
                  inner join research_response.survey_response MSR on char_length(
                    COALESCE(MSR.answer_text, '')
                  ) - char_length(
                    replace(
                      COALESCE(MSR.answer_text, ''), 
                      ',', 
                      ''
                    )
                  ) >= numbers.rn - 1 
                where 
                  MSR.study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                  and MSR.question_id in (
                    ${this.setBindingParams(
                      bindingParams, params.verticalBarchartQuestionIds
                    ) }
                  ) 
                  and MSR.question_text is not null 
                  and MSR.question_text != '' 
                  and MSR.question_id is not null 
                  and MSR.question_id != ''
              ) t1 
              inner join (
                select st1.id,st1.task_instance_id
                from research_response.survey_response_tracker st1
                inner join (select max(completion_time_utc ) as maxtime
                from research_response.survey_response_tracker q1
                where study_id= ${this.setBindingParams(bindingParams, params.studyId) } 
                group by task_instance_id,participant_id,survey_id )st2
                on st1.completion_time_utc=st2.maxtime
                where st1.study_id= ${this.setBindingParams(bindingParams, params.studyId) }
                group by st1.task_instance_id,st1.participant_id,st1.survey_id
              ) t2 on t1.survey_response_tracker_id = t2.id
          ) a 
          left join (
            select 
              task_id, 
              task_title, 
              study_id, 
              participant_id, 
              sequence, 
              start_day, 
              task_instance_id 
            from 
              (
                select 
                  task_id, 
                  task_title, 
                  study_id, 
                  participant_id, 
                  sequence, 
                  start_day, 
                  task_instance_id 
                from 
                  research_response.participant_task_schedule 
                where 
                  study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                group by 
                  task_id, 
                  participant_id, 
                  task_instance_id 
                union all 
                select 
                  task_id, 
                  task_title, 
                  study_id, 
                  participant_id, 
                  sequence, 
                  start_day, 
                  task_instance_id 
                from 
                  research_response.task_schedules_sync 
                where 
                  study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
                group by 
                  task_id, 
                  participant_id, 
                  task_instance_id
              ) b1 
            where 
              study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
            group by 
              task_id, 
              participant_id, 
              task_instance_id
          ) b on a.survey_id = b.task_id 
          and a.study_id = b.study_id 
          and a.participant_id = b.participant_id 
          and a.task_instance_id = b.task_instance_id
      ) q1 
      left join (
        select 
          MSR.participant_id, 
          MSR.question_id, 
          MSR.question_text, 
          COALESCE(MSR.answer_text, '') as answer_text 
        from 
          research_response.survey_response MSR 
        where 
          MSR.study_id = ${this.setBindingParams(bindingParams, params.studyId) } 
          and MSR.participant_id = ${this.setBindingParams(
            bindingParams, params.participantId
          ) } 
          and MSR.question_id in (
            ${this.setBindingParams(
              bindingParams, params.verticalBarchartQuestionIds
            ) }
          ) 
          and MSR.question_text is not null 
          and MSR.question_text != '' 
          and MSR.question_id is not null 
          and MSR.question_id != ''
      ) q2 on q1.questionId = q2.question_id 
      and q1.participant_id = q2.participant_id 
      and q1.choosenAnswer = q2.answer_text
  ) T1 
group by 
  surveyQuestion, 
  questionId, 
  choosenAnswer, 
  milestoneDay 
order by 
  questionId, 
  milestoneDay asc
      `
      console.log(`getParticipantInsightVerticalBarchart query SQL ${params.studyId} \n${querySql}`);
      console.log(`getParticipantInsightVerticalBarchart params: ${JSON.stringify(params)}`);
      console.log(`getParticipantInsightVerticalBarchart binding_params: \n${JSON.stringify(bindingParams)}`);
      console.time(`getParticipantInsightVerticalBarchart query SQL ${params.studyId} Ends in:`)
      const [data] = await dbConnectionPool.query(querySql, bindingParams)
      console.timeEnd(`getParticipantInsightVerticalBarchart query SQL ${params.studyId} Ends in:`);
      dbConnectionPool.end()
      return data
  } catch (er) {
    dbConnectionPool.end()
    console.log('[Error in function getParticipantInsight]', er)
    throw er
}
}
}


module.exports = PatientAnalyticsModel;
