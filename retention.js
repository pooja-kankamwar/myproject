const RetentionModel = require('../models/retention');
const {d3} = require('../packages')
const logger = require('../config/utils');

const getRetentionRate = async (params) => {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}-${params.groupBy}`
  const {clientId} = params;

  try {
    let response = {};
    let result = {};
    const retentionModel = new RetentionModel({clientId});
    const milestoneData = await retentionModel.getMilestoneData(params)
    console.log(`getRetentionRate milestoneData data : ${JSON.stringify(milestoneData)}`)
    if (milestoneData) {
      result = await retentionModel.getMilestoneCohortData(params)
      console.log(`getRetentionRate milestone Cohort Data: ${JSON.stringify(result)}`)
    } else {
      result = await retentionModel.getCustomCohortData(params)
      console.log(`getRetentionRate custom Cohort Data: ${JSON.stringify(result)}`)
    }
    if (!result) {
      return null;
    }

    result = d3.nest().key(d => d.join_date).entries(result).map((row) => {
      let bucketObject = {};
      let totalParticipant = 0;

      row.values.forEach((row) => {
        if (row.bucket_name === "Users") {
          totalParticipant += parseInt(row.participation_rate);
        } else {
          bucketObject = {
            ...bucketObject,
            [row.bucket_name]: parseFloat(row.participation_rate)
          };
        }
      });

      return {
        join_date: row.key,
        participation_rate: bucketObject,
        Users: totalParticipant,
      }
    })

    response.studyId = params.studyId;
    response.data = result;
    return response;
  } catch (error) {
    console.error(`Error in getRetentionRate ${processName}`)
    console.error(error)
    throw error;
  }
};

const getRetentionScore = async (params) => {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}-${params.groupBy}`
  const {clientId} = params;

  try {
    let response = {};
    let result = {};
    const retentionModel = new RetentionModel({clientId});
    const milestoneData = await retentionModel.getMilestoneData(params)
    console.log(`getRetentionScore milestoneData data : ${JSON.stringify(milestoneData)}`)
    if (milestoneData) {
      result = await retentionModel.getRetentionScoreMilestone(params)
      console.log(`getRetentionScore milestone Cohort Data: ${JSON.stringify(result)}`)
    } else {
      result = await retentionModel.getRetentionScoreCustom(params)
      console.log(`getRetentionScore custom Cohort Data: ${JSON.stringify(result)}`)
    }
    if (!result) {
      return null;
    }

    response.studyId = params.studyId;
    response.data = result[0];
    return response;
  } catch (error) {
    console.error(`Error in getRetentionScore ${processName}`)
    console.error(error)
    throw error;
  }
}

const getParticipantStudyProgression = async (params) => {
  const { clientId, siteId, studyId, fromDate, toDate } = params;
  const retentionModel = new RetentionModel({clientId});
  try {
    const datasets = await retentionModel.getStudyProgressionData(params);
    let response = {}, result = {};
    if (datasets.length > 0) {
      let total_days_in_study = 0, average;
      d3.nest()
        .key(d => d.country_name).sortKeys(d3.ascending)
        .key(d => d.site_name).sortKeys(d3.ascending)
        .entries(datasets)
        .map(v=>{
          const country_name = v.key;
          result[country_name] = {}
          v.values.forEach(bySite=> {
            const site_name = bySite.key;
            result[country_name][site_name] = [];
            const participants = [];
            bySite.values.forEach(val => {
              total_days_in_study += Number(val.days_in_study);
              const participant_data = {};
              participant_data.participantId = val.participant_id;
              participant_data.daysInStudy = val.days_in_study;
              participants.push(participant_data);
              result[country_name][site_name] = participants;
            })
          });
        average = total_days_in_study/datasets.length;
        average = Math.round(average * 100) / 100;
        response.averageStudyProgression = average;
        response.countries = result;
        return v
      })
    } else {
      response = null;
    }
    return response;
  } catch (error) {
    logger.createLog('', `Error in getParticipantStudyProgression`, error);
    throw error;
  }
}

module.exports = {
  getRetentionRate,
  getRetentionScore,
  getParticipantStudyProgression
};
