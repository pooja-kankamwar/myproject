const CommonsModel = require('../models/commons')
const utils = require('../config/utils');
const CONST_VAL = require('../constants');
const constants = require('../config/constants');
const s3Services = require('../utils/s3Services');
const EdroDashboardhModel = require('../models/edroDashboard');
const { d3, moment, uuid, lodash } = require("../packages");
//const utils = require('../libraries/datetimeLib')
const date_moment = require('moment');

const getEDROActivities = async (event) => {

    let authorizationResponse = utils.authorization(event);
    console.log("auth status --", authorizationResponse);
    if (authorizationResponse.statusCode != constants.statusCode.Succes) {
        return authorizationResponse;
    }
    let clientId = (event.headers.clientid) ? event.headers.clientid : event.headers.clientId;
    let studyId = event.pathParameters.studyid;
    let s3ClientBucket = null;
    let eDROActivities = [];
    let response = {};
    let analyticsDashboardEnabledActivities = [];
    try {
        const commonsModel = new CommonsModel({
            clientId
        });
        const dbConnectionPool = await commonsModel._initDbConnectionPool(clientId, CONST_VAL.constants.DATABASE.RESEARCH_DB);
        const bindingParams = []
        bindingParams.push(studyId)
        let querySql = `
            select activity_id, activity_title 
            from research.study_activity_meta_data
            where study_id = ? `
        const [studyActivities] = await dbConnectionPool.query(querySql, bindingParams)
        console.log("studyActivity" + JSON.stringify(studyActivities))
        if (studyActivities) {
            const [clientData] = await dbConnectionPool.query(`select * from research.client_config cc where cc.id = ${clientId}`)
            const clientConfig = clientData[0];
            if (!clientConfig) {
                throw new Error('Client config not found')
            }
            s3ClientBucket = clientConfig.s3_bucket;

            const s3BucketExists = await s3Services.checkBucketExists(s3ClientBucket);
            if (!s3BucketExists) {
                throw new Error('Client S3 bucket not found.');
            }

            const [clientActivities] = await dbConnectionPool.query(`select * from research.client_activity`);
            if (!clientActivities) {
                throw new Error('Client activities not found')
            }

            for (let activity of clientActivities) {
                console.log("activity " + JSON.stringify(activity))
                const eDROActivityKey = CONST_VAL.constants.S3_STATIC_FOLDER.EDROs + '/' + activity.id + '.json'
                console.log("eDROActivityKey " + JSON.stringify(eDROActivityKey))
                const eDroActivityExists = await s3Services.doesObjectExists(eDROActivityKey, s3ClientBucket);
                if (eDroActivityExists) {
                    console.log('Fetching eDRO Activity Data - ', eDroActivityExists);
                    const s3EDROActivity = await s3Services.readS3JSONObject(eDROActivityKey, s3ClientBucket);
                    if (s3EDROActivity instanceof Error) {
                        console.log('Error fetching eDRO Activity Data for - ', eDROActivityKey);
                        continue;
                    }
                    const eDROActivity = JSON.parse(s3EDROActivity)
                    if (eDROActivity.eDRODashboardEnabled && eDROActivity.eDRODashboardEnabled === true) {
                    console.log("S3 eDROActivity" + JSON.stringify(eDROActivity))
                    analyticsDashboardEnabledActivities.push(eDROActivity.id);
                     }

                }
            }
            console.log("analyticsDashboardEnabledActivities" + JSON.stringify(analyticsDashboardEnabledActivities))
            for (let studyActivity of studyActivities) {
                const studyActivityKey = 'studies/' + studyId + '/' + 'activities' + '/' + studyActivity.activity_id + '.json'

                const studyActivityExists = await s3Services.doesObjectExists(studyActivityKey, s3ClientBucket);
                if (studyActivityExists) {
                    const activityS3Obj = await s3Services.readS3JSONObject(studyActivityKey, s3ClientBucket);
                    if (activityS3Obj instanceof Error) {
                        console.log('Error fetching  Activity Data for - ', studyActivityKey);
                        continue;
                    }
                    const activityData = JSON.parse(activityS3Obj)
                    console.log("activityS3Obj " + JSON.stringify(activityData))
                    if (analyticsDashboardEnabledActivities.includes(activityData.id)) {
                    let responseActivity = {
                        activityId: activityData.identifier,
                        activityTitle: activityData.title,
                        filters: activityData.dataCollected.sort()
                    }
                    eDROActivities.push(responseActivity);
                    }
                }
            }
        }
    } catch
        (error) {
        console.log('Error in fetching eDRO Activities:', error);
        throw error;
    }
    response.activities = eDROActivities;
    return utils.success(constants.statusCode.Succes, constants.status.SUCCESS, response);
}

const getRecentData = async (params) => {
    params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
    params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;

    let limit = 200;
    let offset = 0;
    if ( params.pageSize) {
        limit = Number(params.pageSize);
    }
    if ( params.pageNum) {
        offset = Number(params.pageNum);
        if (offset > 0){
            offset = offset * limit;
        }
    }
    params.limit = limit;
    params.offset = offset;
    params.sortValue = "start_time";
    params.sortOrder = "DESC";
    let response = [];
    const {clientId} = params;
    const eDRODashboardModel = new EdroDashboardhModel({clientId});
    let queryData = [];
    try {
        queryData = await eDRODashboardModel.getActivityResponseData(params);
    } catch (error) {
        console.error(`Error in getRecentData`, error)
        throw error;
    }
    for (let recordData of queryData) {
        try {
            let readings = await recentDataForForcedSpirometry(recordData.response_data, recordData.participantId,recordData.start_time)
            response.push(...readings)
        } catch (error) {
            console.error(`Error in processing records` + recordData, error)
            continue;
        }
    }
    return utils.success(constants.statusCode.Succes, constants.status.SUCCESS, response);
}

function recentDataForForcedSpirometry(data, participantId,startDate) {

    let recentData = []
    startDate = moment(startDate).format("DD MMM YYYY T HH:mm:ss").toString().toUpperCase();
    const eDROResponse = JSON.parse(data);
    console.log('eDROResponse', eDROResponse)
    const deviceName = eDROResponse['deviceName']
    let fvc = eDROResponse['fvc'];
    if (fvc) {
        for (let i of fvc) {
            let reading = i['reading']
            let result = i['result']
            let qualityKey = result['quality_key']
            let qualityMessage = '';
            if (qualityKey) {
               qualityMessage = CONST_VAL.constants.SPIROMETER_QUALITY_MESSAGES[qualityKey] ? CONST_VAL.constants.SPIROMETER_QUALITY_MESSAGES[qualityKey] : '';
            }
            let attributes = result['attributes'];
            let data = {
                participantId,
                deviceName,
                reading,
                qualityMessage,
                startDate
            }
            for (let att of attributes) {
                data[att.attribute.toLowerCase()] = att['value']
                let unit = att.attribute + '_' + 'unit';
                data[unit.toLowerCase()] = att['unit']
            }
            recentData.push(data)
        }
    }

    return recentData
}

const getEdroActivitiesExpectedData = async (params) => {
    utils.createLog('', `Begin getEdroActivitiesExpectedData`, params);
    const { clientId } = params;
     try {
      const edroDashboardhModel = new EdroDashboardhModel({ clientId });
      let queriesData = await edroDashboardhModel.getExpectedData(params);
      if (queriesData) {
          queriesData.map( querieData=> {
          querieData.expected_missing_date= querieData.expected_missing_date ?  date_moment(querieData.expected_missing_date).format("DD MMM YYYY").toString().toUpperCase() : null;
          querieData.most_recent_data= querieData.most_recent_data? date_moment(querieData.most_recent_data).format("DD MMM YYYY").toString().toUpperCase() :null;
          if(querieData.user_defined_participant_id){
             querieData.participantId = querieData.user_defined_participant_id;          
            }
            
            delete querieData.discontinued_date;
            delete querieData.start_date_utc;
            delete querieData.participant_id;

            delete querieData.user_defined_participant_id;
            delete querieData.taskId;
            delete querieData.study_id;
          });
        return queriesData
      }
      else{
         return [];
      }
     } catch (error) {
      utils.createLog('', `Error in getEdroActivitiesExpectedData`, error);
      throw error;
    }
  };

  const getDailyReadingData = async (params) => {
    let response = {},queryData = [];
    const { clientId } = params;
    const eDRODashboardModel = new EdroDashboardhModel({ clientId });
    try {
      queryData = await eDRODashboardModel.getActivityResponseData(params);
      response =  dailySpirometerReadingData(queryData, params);
    } catch (error) {
      console.error(`Error in getDailyReadingData`, error);
      throw error;
    }
  
    return utils.success(
      constants.statusCode.Succes,
      constants.status.SUCCESS,
      response
    );
  };

function dailySpirometerReadingData(responseData, params) {
  let result = {};
  for (let recordData of responseData) {

    let readings = {},
      totalReading = 0,
      recordDate;
    recordDate = moment(recordData.end_time).format("DD MMM YYYY").toString().toUpperCase();
    const jsonData = JSON.parse(recordData.response_data);
    if (!jsonData || !jsonData.fvc || jsonData.fvc.length <= 0) {
      continue
    }
    jsonData.fvc.map((data, index) => {
      if (jsonData.fvc.length && jsonData.fvc.length == index + 1) {
        totalReading = Number(data.reading);
      }
      data.result.attributes.map((attributeObj) => {
        if (!readings[attributeObj.attribute]) {
          readings[attributeObj.attribute] = 0
        }
        readings[attributeObj.attribute] = Number(attributeObj.value);
        let findObj = getMatchingRecord(result, attributeObj.attribute, recordData.participantId, recordDate)
        findObj.total_readings += readings[attributeObj.attribute]
        findObj.number_of_readings += totalReading;

      });
    });

  }
  for (let r in result) {
    result[r].map(d => { d.mean_reading = Math.round(d.total_readings / d.number_of_readings) })
  }

  return result;
};
  
  const getMatchingRecord = (result, activityAttribute, participantId, dateTime) => {
    if (!result[activityAttribute]) {
      result[activityAttribute] = []
    }
    let findObj = result[activityAttribute].find((singleObj) =>
      singleObj.participant_id === participantId &&
      singleObj.date === dateTime
    );
    if (!findObj) {
      findObj = {
        total_readings: 0,
        participant_id: participantId,
        date: dateTime,
        mean_reading: 0,
        number_of_readings: 0
      }
      result[activityAttribute].push(findObj)
    }
    return findObj
  }

module.exports = {
    getEDROActivities,
    getRecentData,
    getEdroActivitiesExpectedData,
    getDailyReadingData,
}