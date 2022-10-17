const CommonsModel = require('../models/commons');
const Redis = require("../config/redis");
const {d3} = require('../packages')
const utils = require('../utils');
const transferType = "LOKAVANT";


const getAllUsers = async (clientId, studyId, siteId)=> {
  let result = {};
  const commonsModel = new CommonsModel({
    clientId
  });
  const complianceData = await commonsModel.getAllUsersData(studyId, siteId)
  return complianceData;
}

const initiateAnalytics = async (clientId, studyId)=> {
  const REDIS_KEY_ANALYTICS = '_ANALYTICS_IN_PROGRESS';
  let result = {};
  let redisInsert = null;
  
  // const commonsModel = new CommonsModel({
  //   clientId
  // });
  const redis = new Redis();
  const redisClient = await redis._initRedisConnectionPool();
  console.log(`initiate connection in service: ${JSON.stringify(redisClient)}`);
  let redisKey = studyId + REDIS_KEY_ANALYTICS;
  console.log(`key: ${redisKey}`);
  
  let isInProgress = await redis._retrieveData(redisClient,redisKey);
  console.log(`isInProgress: ${isInProgress}`);
  if (!isInProgress) {
    redisInsert = await redis._insertData(redisClient, redisKey, 1);
    console.log(`result insert data: ${redisInsert}`);
    isInProgress = await redis._retrieveData(redisClient,redisKey);
    console.log(`isInProgress after insert: ${isInProgress}`);
  }
  redisClient.quit();

  if (isInProgress) {
    result[redisKey] = isInProgress;
    result['expire_time'] = redis._expireTime;
  }

  // const complianceData = await commonsModel.initiateAnalytics(studyId, siteId)
  return result;
}

const getbenchmarkDataConfig = async (clientId, studyId)=> {
  const commonsModel = new CommonsModel({
    clientId
  });
  let res = { enabled:false, targetUrl: ""};
  const data = await commonsModel.getbenchmarkData(studyId, transferType)
  if(data.length > 0)
  {
    res.enabled = true;
    res.targetUrl = data[0].targetUrl
    return res;
  }else{
    return res;
  }
 
}

const getMilestones = async (params)=> {
  const { clientId,studyId } = params;
  let response={};
  let dataset = [];
  const results = [];
  let studyDays;
  const commonsModel = new CommonsModel({clientId});
  dataset = await commonsModel.getMilestones(studyId)
  if(dataset.length > 0){
    dataset.forEach(data => {
      const result = {};
      const milestone_name = data.milestone_name;
      const milestone_day = data.milestone_day;
      studyDays = data.study_length
      result[milestone_name] = milestone_day;
      if(milestone_name != null && milestone_day != null)
        results.push(result)
    })
    return {
      studyDays:studyDays,
      milestones:results
    };
  }else{
    return response;
  }
}

module.exports = {
  getAllUsers,
  initiateAnalytics,
  getbenchmarkDataConfig,
  getMilestones
}
