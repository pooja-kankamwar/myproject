const DataCollectionModel = require('../models/dataCollection');
const {d3, moment, uuid} = require('../packages')
const utils = require('../libraries/datetimeLib')

const getQueriesData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let queriesData = await dataCollectionModel.getQueriesData(params)
    if (!queriesData) {
      return null;
    }
    queriesData = d3.nest()
                  .key(d => d.activityName).sortKeys(d3.ascending)
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .key(d => d.bucketType).sortKeys(d3.ascending)
                  .entries(queriesData)
                  .map(v=>{
                    const activityName = v.key;
                    result[activityName] = {}
                    v.values.forEach(byCountry=> {
                      const countryName = byCountry.key;
                      result[activityName][countryName] = {}
                      byCountry.values.forEach(bySite=>{
                        const siteName = bySite.key;
                        result[activityName][countryName][siteName] = {}
                        bySite.values.forEach(byBucketType=> {
                          const bucketType = byBucketType.key;
                          result[activityName][countryName][siteName][bucketType] = {}
                          byBucketType.values.forEach(val=>{
                            const queryData = {}
                            queryData.queryOpen = val.n_queries_open;
                            queryData.queryResponded = val.n_responded_not_closed;
                            queryData.queryClosed = val.n_closed_query;
                            result[activityName][countryName][siteName][bucketType] = queryData;
                          })
                        })
                      })
                    });
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.activities = result;
    return response;
  } catch (error) {
    console.error(`Error in getQueriesData ${processName}`)
    console.log(error)
    throw error;
  }
}

const getActivitiesData = async (params)=> {
  let tempTimeActivitiesData = {};
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let [timeActivitiesData, activitiesData] = await Promise.all([
      dataCollectionModel.getActivitiesByTimeDIffData(params),
      dataCollectionModel.getActivitiesData(params)
    ])
    timeActivitiesData = d3.nest()
                  .key(d => d.activityName).sortKeys(d3.ascending)
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(timeActivitiesData)
                  .map(v=>{
                    const activityName = v.key;
                    tempTimeActivitiesData[activityName] = {}
                    v.values.forEach(byCountry=> {
                      const countryName = byCountry.key;
                      tempTimeActivitiesData[activityName][countryName] = {}
                      byCountry.values.forEach(bySite=>{
                        const siteName = bySite.key;
                        tempTimeActivitiesData[activityName][countryName][siteName] = {}
                        bySite.values.forEach(val=> {
                          const activityData = {}
                          activityData.hoursEntryCompleted = val.secondEntryCompleted > 0 && val.secondEntryCompleted < 3600 ? 1 : Number(Math.floor(val.secondEntryCompleted / 3600));
                          activityData.hoursApproved = val.secondApproved > 0 && val.secondApproved < 3600 ? 1 : Number(Math.floor(val.secondApproved / 3600));
                          activityData.hoursVerified = val.secondVerified > 0 && val.secondVerified < 3600 ? 1 : Number(Math.floor(val.secondVerified / 3600));
                          activityData.hoursSigned = val.secondSigned > 0 && val.secondSigned < 3600 ? 1 : Number(Math.floor(val.secondSigned / 3600));
                          tempTimeActivitiesData[activityName][countryName][siteName] = activityData;
                        })
                      })
                    });
                    delete v.key;
                    delete v.values;
                    return v
                  })

    activitiesData = d3.nest()
                    .key(d => d.activityName).sortKeys(d3.ascending)
                    .key(d => d.countryName).sortKeys(d3.ascending)
                    .key(d => d.siteName).sortKeys(d3.ascending)
                    .entries(activitiesData)
                    .map(v=>{
                      const activityName = v.key;
                      result[activityName] = {}
                      //below to combine other query result
                      if (!tempTimeActivitiesData[activityName]) {
                        tempTimeActivitiesData[activityName] = {}
                      }
                      v.values.forEach(byCountry=> {
                        const countryName = byCountry.key;
                        result[activityName][countryName] = {}
                        if (!tempTimeActivitiesData[activityName][countryName]) {
                          tempTimeActivitiesData[activityName][countryName] = {}
                        }
                        byCountry.values.forEach(bySite=>{
                          const siteName = bySite.key;
                          result[activityName][countryName][siteName] = {}
                          if (!tempTimeActivitiesData[activityName][countryName][siteName]) {
                            tempTimeActivitiesData[activityName][countryName][siteName] = {}
                            tempTimeActivitiesData[activityName][countryName][siteName].hoursEntryCompleted = 0;
                            tempTimeActivitiesData[activityName][countryName][siteName].hoursApproved = 0;
                            tempTimeActivitiesData[activityName][countryName][siteName].hoursVerified = 0;
                            tempTimeActivitiesData[activityName][countryName][siteName].hoursSigned = 0;
                          }
                          bySite.values.forEach(val=> {
                            const activityData = {}
                            activityData.totalEntry = val.totalEntry;
                            activityData.totalApproved = val.totalApproved;
                            activityData.totalVerified = val.totalVerified;
                            activityData.totalSigned = val.totalSigned;
                            activityData.percentageEntry = val.percentageEntry;
                            activityData.percentageApprove = val.percentageApprove;
                            activityData.percentageVerify = val.percentageVerify;
                            activityData.percentageSigned = val.percentageSigned;
                            //below to combine other query result (timeActivitiesData)
                            activityData.hoursEntryCompleted = tempTimeActivitiesData[activityName][countryName][siteName].hoursEntryCompleted;
                            activityData.hoursApproved = tempTimeActivitiesData[activityName][countryName][siteName].hoursApproved;
                            activityData.hoursVerified = tempTimeActivitiesData[activityName][countryName][siteName].hoursVerified;
                            activityData.hoursSigned = tempTimeActivitiesData[activityName][countryName][siteName].hoursSigned;
                            result[activityName][countryName][siteName] = activityData;
                          })
                        })
                      });
                      delete v.key;
                      delete v.values;
                      return v
                    })
    response.studyId = params.studyId;
    response.activities = result;
    return response;
  } catch (error) {
    console.error(`Error in getActivitiesData ${processName}`)
    console.log(error)
    throw error;
  }
}

const getCompletionData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let completionData = await dataCollectionModel.getCompletionData(params)
    completionData = d3.nest()
                  .key(d => d.activityName).sortKeys(d3.ascending)
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(completionData)
                  .map(v=>{
                    const activityName = v.key;
                    result[activityName] = {}
                    v.values.forEach(byCountry=> {
                      const countryName = byCountry.key;
                      result[activityName][countryName] = {}
                      byCountry.values.forEach(bySite=>{
                        const siteName = bySite.key;
                        result[activityName][countryName][siteName] = {}
                        bySite.values.forEach(val=> {
                          const completionData = {}
                          completionData.nTotalCompleted = val.nTotalCompleted;
                          completionData.nTotalMissing = val.nTotalMissing;
                          completionData.nTotalCompletedPrev1Week = val.nTotalCompletedPrev1Week;
                          completionData.nTotalMissingPrev1Week = val.nTotalMissingPrev1Week;
                          result[activityName][countryName][siteName] = completionData;
                        })
                      })
                    });
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.activities = result;
    return response;
  } catch (error) {
    console.error(`Error in getCompletedDataCollection ${processName}`)
    console.log(error)
    throw error;
  }
}

const getTotalDataChange = async (params) => {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId} = params;
  try {
    
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let totalDataChangeData = await dataCollectionModel.getTotalDataChange(params)
    totalDataChangeData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(totalDataChangeData)
                  .map(v=>{
                    const countryName = v.key;
                    result[countryName] = {}
                    v.values.forEach(bySite=>{
                      const siteName = bySite.key;
                      result[countryName][siteName] = {}
                      bySite.values.forEach(val=> {
                        const totalDataChangeData = {}
                        totalDataChangeData.totalDataChange = val.total_data_change;
                        totalDataChangeData.totalDataChangePrev1Week = val.total_data_change_prev1wk;
                        result[countryName][siteName] = totalDataChangeData;
                      })
                    })
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.locations = result;
    return response;
  } catch (error) {
    console.error(`Error in getTotalDataChange ${processName}`)
    console.log(error)
    throw error;
  }
}

const getTotalQueryData = async (params)=> {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId} = params;
  try {

    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let totalQueryData = await dataCollectionModel.getTotalQueryData(params)
    totalQueryData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(totalQueryData)
                  .map(v=>{
                    const countryName = v.key;
                    result[countryName] = {}
                    v.values.forEach(bySite=>{
                      const siteName = bySite.key;
                      result[countryName][siteName] = {}
                      bySite.values.forEach(val=> {
                        const totalQueryData = {}
                        totalQueryData.totalQuery = val.total_query;
                        totalQueryData.totalQueryPrev1Week = val.total_query_prev1wk;
                        result[countryName][siteName] = totalQueryData;
                      })
                    })
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.locations = result;
    return response;
  } catch (error) {
    console.error(`Error in getTotalQueryData ${processName}`)
    console.log(error)
    throw error;
  }
}

const getMissingData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let missingData = await dataCollectionModel.getMissingData(params)
    missingData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(missingData)
                  .map(v=>{
                    const countryName = v.key;
                    result[countryName] = {}
                    v.values.forEach(bySite=> {
                      const siteName = bySite.key;
                      result[countryName][siteName] = {}
                      bySite.values.forEach(val=> {
                        result[countryName][siteName]['0-7 days'] = {}
                        result[countryName][siteName]['0-7 days']['totalMissing'] = val.n_missed_7_days;
                        result[countryName][siteName]['>7-30 days'] = {}
                        result[countryName][siteName]['>7-30 days']['totalMissing'] = val.n_missed_7_30_days;
                        result[countryName][siteName]['>30-60 days'] = {}
                        result[countryName][siteName]['>30-60 days']['totalMissing'] = val.n_missed_30_60_days;
                        result[countryName][siteName]['>60 days'] = {};
                        result[countryName][siteName]['>60 days']['totalMissing'] = val.n_missed_60_days;
                      })
                    });
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.locations = result;
    return response;
  } catch (error) {
    console.error(`Error in getCompletedDataCollection ${processName}`)
    console.log(error)
    throw error;
  }
}

const getFormStatusData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let formStatusData = await dataCollectionModel.getFormStatusData(params)
    formStatusData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .key(d => d.bucketType).sortKeys(d3.ascending)
                  .entries(formStatusData)
                  .map(v=>{
                    const countryName = v.key;
                    result[countryName] = {}
                    v.values.forEach(bySite=> {
                      const siteName = bySite.key;
                      result[countryName][siteName] = {}
                      bySite.values.forEach(byBucketType => {
                        const bucketName = byBucketType.key;
                        result[countryName][siteName][bucketName] = {}
                        byBucketType.values.forEach(val=> {
                          const formStatusData = {}
                          formStatusData.totalEntry = val.totalEntry;
                          formStatusData.totalApproved = val.totalApproved;
                          formStatusData.totalVerified = val.totalVerified;
                          formStatusData.totalSigned = val.totalSigned;
                          result[countryName][siteName][bucketName] = formStatusData;
                        })
                      })
                    });
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.locations = result;
    return response;
  } catch (error) {
    console.error(`Error in getFormStatusData ${processName}`)
    console.log(error)
    throw error;
  }
}

const getQueriesTimeData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let queriesTimeData = await dataCollectionModel.getQueriesTimeData(params)
    queriesTimeData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(queriesTimeData)
                  .map(v=>{
                    const countryName = v.key;
                    result[countryName] = {}
                    v.values.forEach(bySite=> {
                      const siteName = bySite.key;
                      result[countryName][siteName] = {}
                      bySite.values.forEach(val => {
                        const queriesTimeData = {}
                        queriesTimeData.avgHourQueriesClose = val.avgHourQueriesClose;
                        queriesTimeData.longestActivity = val.longestActivity;
                        result[countryName][siteName] = queriesTimeData;
                      })
                    });
                    delete v.key;
                    delete v.values;
                    return v
                  })
    response.studyId = params.studyId;
    response.locations = result;
    return response;
  } catch (error) {
    console.error(`Error in getQueriesTimeData Service ${processName}`)
    console.log(error)
    throw error;
  }
}

const getParticipantCompletionData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const dataCollectionModel = new DataCollectionModel({clientId});
    let participantCompletionData = await dataCollectionModel.getParticipantCompletionData(params)
    participantCompletionData = d3.nest()
                              .key(d => d.countryName).sortKeys(d3.ascending)
                              .key(d => d.siteName).sortKeys(d3.ascending)
                              .key(d => d.participantId).sortKeys(d3.ascending)
                              .entries(participantCompletionData)
                              .map(v=>{
                                const countryName = v.key;
                                result[countryName] = {}
                                v.values.forEach(bySite=> {
                                  const siteName = bySite.key;
                                  result[countryName][siteName] = {}
                                  bySite.values.forEach(byParticipant => {
                                    const participantId = byParticipant.key;
                                    result[countryName][siteName][participantId] = {}
                                    byParticipant.values.forEach(val=>{
                                      const participantCompletionData = {}
                                      participantCompletionData.avgHourQueriesClose = val.totalCompleted;
                                      participantCompletionData.longestActivity = val.totalMissing;
                                      result[countryName][siteName][participantId] = participantCompletionData;
                                    })
                                  })
                                });
                                delete v.key;
                                delete v.values;
                                return v
                              })
    response.studyId = params.studyId;
    response.locations = result;
    return response;
  } catch (error) {
    console.error(`Error in getParticipantCompletionData ${processName}`)
    console.log(error)
    throw error;
  }
}

module.exports = {
  getQueriesData,
  getActivitiesData,
  getCompletionData,
  getTotalQueryData,
  getTotalDataChange,
  getMissingData,
  getFormStatusData,
  getQueriesTimeData,
  getParticipantCompletionData,
}