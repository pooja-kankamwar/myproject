const StudyComplianceModel = require('../models/studyCompliance');
const CommonModel = require('../models/commons');
const {d3, moment, uuid} = require('../packages')
const utils = require('../libraries/datetimeLib')
const logger = require('../config/utils');

const mappingComplianceChangeScoreData = (changeScoreDatas = [], deviationDatas = []) => {
  const results = [];
  let minDate = null;
  let maxDate = null;
  let minDateTotal = null;
  let maxDateTotal = null;

  minDate = changeScoreDatas[0].date;
  maxDate = changeScoreDatas[changeScoreDatas.length - 1].date;

  minDateTotal = moment(minDate, 'YYYY-MM-DD').startOf('month');
  maxDateTotal = moment(maxDate, 'YYYY-MM-DD').startOf('month');

  for (const m = moment(minDateTotal); m.diff(maxDateTotal, 'months') <= 0; m.add(1, 'months')) {
    const month = m.month() + 1;
    const year = m.year();
    const monthYearString = m.format('YYYY-MM')
    let changeScoreDataInMonth = changeScoreDatas.filter(d => d.month === month && d.year === year);
    let deviationDataInMonth = deviationDatas.filter(d => d.month === month && d.year === year);
    if (changeScoreDataInMonth.length === 0 && deviationDataInMonth.length === 0) {
      continue;
    }
    if (changeScoreDataInMonth && changeScoreDataInMonth.length > 0) {
      changeScoreDataInMonth.forEach(data => {
        const result = {};
        const countryName = data.countryName;
        const siteName = data.siteName;
        const percentChange = data.percentChange;
        let pairData = null;
        let standardDeviation = null;
        const pairDataIndex = deviationDataInMonth.findIndex(d => d.countryName === countryName && d.siteName == siteName);
        if (pairDataIndex > -1) { // this will find the completion data in the same country, site, date
          [pairData] = deviationDataInMonth.splice(pairDataIndex, 1);
        }
        
        if (pairData) {
          standardDeviation = pairData.standardDeviation;
        }
        result.date = monthYearString;
        result.countryName = countryName;
        result.siteName = siteName;
        result.percentChange = percentChange;
        result.standardDeviation = standardDeviation;
        results.push(result)
      })
    }
    
  }
  return results;
}

const groupingComplianceData = (complianceDatas = [], completionDatas = [], groupBy = null) => {
  const results = [];
  let minDateCompliance = null;
  let minDateCompletion = null;
  let maxDateCompliance = null;
  let maxDateCompletion = null;
  let minDateTotal = null;
  let maxDateTotal = null;

  if (complianceDatas && complianceDatas.length > 0) {
    minDateCompliance = complianceDatas[0].date;
    maxDateCompliance = complianceDatas[complianceDatas.length - 1].date;
  }
  if (completionDatas && completionDatas.length > 0) {
    minDateCompletion = completionDatas[0].date;
    maxDateCompletion = completionDatas[completionDatas.length - 1].date;
  }
  if (minDateCompletion && maxDateCompletion && minDateCompliance && maxDateCompliance) {
    minDateTotal = moment(minDateCompliance, 'YYYY-MM-DD').isBefore(moment(minDateCompletion, 'YYYY-MM-DD')) ? moment(minDateCompliance, 'YYYY-MM-DD').startOf('month')
                  : moment(minDateCompletion, 'YYYY-MM-DD').startOf('month');
    maxDateTotal = moment(maxDateCompliance, 'YYYY-MM-DD').isAfter(moment(maxDateCompletion, 'YYYY-MM-DD')) ? moment(maxDateCompliance, 'YYYY-MM-DD').startOf('month') 
                  : moment(maxDateCompletion, 'YYYY-MM-DD').startOf('month');
  } else if (minDateCompliance && maxDateCompliance) {
    minDateTotal = moment(minDateCompliance, 'YYYY-MM-DD').startOf('month');
    maxDateTotal = moment(maxDateCompliance, 'YYYY-MM-DD').startOf('month');
  } else if (minDateCompletion && maxDateCompletion) {
    minDateTotal = moment(minDateCompletion, 'YYYY-MM-DD').startOf('month');
    maxDateTotal = moment(maxDateCompletion, 'YYYY-MM-DD').startOf('month');
  }
  
  for (const m = moment(minDateTotal); m.diff(maxDateTotal, 'months') <= 0; m.add(1, 'months')) {
    const month = m.month() + 1;
    const year = m.year();
    const monthYearString = m.format('YYYY-MM')
    let complianceDataInMonth = complianceDatas.filter(d => d.month === month && d.year === year);
    let completionDataInMonth = completionDatas.filter(d => d.month === month && d.year === year);
    if (complianceDataInMonth.length === 0 && completionDatas.length === 0) {
      continue;
    }
    if (complianceDataInMonth && complianceDataInMonth.length > 0) {
      complianceDataInMonth.forEach(data => {
        const result = {};
        const countryName = data.countryName;
        const siteName = data.siteName;
        let complianceColumnList = [];
        let complianceValueList = [];
        let completionColumnList = [];
        let completionValueList = [];
        let completionPairData = null;
        const completionPairDataIndex = completionDataInMonth.findIndex(d => d.countryName === countryName && d.siteName == siteName);
        if (completionPairDataIndex > -1) { // this will find the completion data in the same country, site, date
          [completionPairData] = completionDataInMonth.splice(completionPairDataIndex, 1);
        }
        Object.entries(data).forEach(d=>{
          if(d[0].includes('compliance-') && d[1] != 0 && d[1] != null ) {
            complianceColumnList.push(d[0]);
            complianceValueList.push(d[1]);
          } 
        });
        if (completionPairData) {
          Object.entries(completionPairData).forEach(d=>{
            if(d[0].includes('completion-') && d[1] != 0 && d[1] != null ) {
              completionColumnList.push(d[0]);
              completionValueList.push(d[1]);
            }
          });
        }
        result.date = monthYearString;
        result.countryName = countryName;
        result.siteName = siteName;
        result.complianceColumnList = complianceColumnList.join(';');
        result.complianceValueList = complianceValueList.join(';');
        result.completionColumnList = completionColumnList.join(';');
        result.completionValueList = completionValueList.join(';');
        results.push(result)
      })
    }
    if (completionDataInMonth && completionDataInMonth.length > 0) { // execute the rest of the unprocessed completion Data
      completionDataInMonth.forEach(data => {
        const result = {};
        const countryName = data.countryName;
        const siteName = data.siteName;
        const complianceColumnList = [];
        const complianceValueList = [];
        const completionColumnList = [];
        const completionValueList = [];
        Object.entries(data).forEach(d=>{
          if(d[0].includes('completion-') && d[1] != 0 && d[1] != null ) {
            completionColumnList.push(d[0]);
            completionValueList.push(d[1]);
          } 
        });
        result.date = monthYearString;
        result.countryName = countryName;
        result.siteName = siteName;
        result.complianceColumnList = complianceColumnList.join(';');
        result.complianceValueList = complianceValueList.join(';');
        result.completionColumnList = completionColumnList.join(';');
        result.completionValueList = completionValueList.join(';');
        results.push(result)
      })
    }
  }
  return results;
}

const getComplianceData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}-${params.groupBy}`
  const {clientId, siteId, studyId, fromDate, toDate, date, groupBy} = params;
  try {
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({
      clientId
    });
    let complianceData = null;
    let completionData = null;
    if (groupBy && groupBy === 'participant') {
      [completionData, complianceData] = await Promise.all([
        studyComplianceModel.getCompletionByParticipant(params),
        studyComplianceModel.getComplianceByParticipant(params)
      ])
    } else {
      [completionData, complianceData] = await Promise.all([
        studyComplianceModel.getCompletion(params),
        studyComplianceModel.getCompliance(params)
      ])
    }
    if ((complianceData === null || complianceData.length <= 0) && (completionData === null || completionData.length <= 0)) {
      return null; // handled in caller
    }
    
    const respData = await groupingComplianceData(complianceData, completionData, groupBy);
    const replacer = (key, value) => value === null ? '' : value // specify how you want to handle null values here
    const header = Object.keys(respData[0])
    let csv = respData.map(row => header.map(fieldName => JSON.stringify(row[fieldName], replacer)).join(','))
    csv.unshift(header.join(','))
    csv = csv.join('\r\n')

    return csv;
  } catch (error) {
    console.error(`Error in getComplianceData ${processName}`)
    console.error(error)
    throw error;
  }
}

const getComplianceDetailsData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate, groupBy, show} = params;
  try {
    let response = [];
    let result = {};
    const participantMode = params.groupBy && params.groupBy === 'participant';
    const shows = params.show && params.show === 'completion' ? 'percentageCompletionRate' : 'percentageComplianceRate';
    const metric = participantMode ? 'participantId' : 'activityName';
    let dateFrom = moment(fromDate, 'YYYY-MM-DD');
    let dateTo = moment(toDate, 'YYYY-MM-DD')
    const studyComplianceModel = new StudyComplianceModel({
      clientId
    });
    let complianceDetailsData = null;
    if (params.show === 'completion') {
      if (participantMode) {
        complianceDetailsData = await studyComplianceModel.getCompletionDetailsByParticipant(params);
      } else {
        complianceDetailsData = await studyComplianceModel.getCompletionDetails(params);
      }
    }
    else {
      if (participantMode) {
        complianceDetailsData = await studyComplianceModel.getComplianceDetailsByParticipant(params);
      } else {
        complianceDetailsData = await studyComplianceModel.getComplianceDetails(params);
      }
    }
    
    complianceDetailsData.forEach(sqlData => {
      const responseData = {};
      const momentDate = moment(sqlData.complianceDate, 'YYYY-MM')
      const date = momentDate.format('YYYY-MM-DD')
      responseData.date = date;
      responseData[metric] = sqlData[metric];
      responseData[shows] = Math.round(sqlData.percentCompliance);
      response.push(responseData);
    });
    return response;
  } catch (error) {
    console.error(`Error in getParticipantComplianceDetailsData ${processName}`)
    console.log(error)
    throw error;
  }
}

const getQueriesData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let queriesData = await studyComplianceModel.getQueriesData(params)
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
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let [timeActivitiesData, activitiesData] = await Promise.all([
      studyComplianceModel.getActivitiesByTimeDIffData(params),
      studyComplianceModel.getActivitiesData(params)
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
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let completionData = await studyComplianceModel.getCompletionData(params)
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

const getMissingData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let missingData = await studyComplianceModel.getMissingData(params)
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
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let formStatusData = await studyComplianceModel.getFormStatusData(params)
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
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let queriesTimeData = await studyComplianceModel.getQueriesTimeData(params)
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
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let response = {};
    let result = {};
    const studyComplianceModel = new StudyComplianceModel({clientId});
    let participantCompletionData = await studyComplianceModel.getParticipantCompletionData(params)
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

const getComplianceRiskScoreData = async (params)=> {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const studyComplianceModel = new StudyComplianceModel({clientId});
  try {
    const datasets = await studyComplianceModel.getComplianceRiskScore(params);
    let result = {};

    if (datasets.length > 0) {
      // map object
      d3.nest()
        .key(d => d.countryName).sortKeys(d3.ascending)
        .key(d => d.siteName).sortKeys(d3.ascending)
        .key(d => d.activityName).sortKeys(d3.ascending)
        .entries(datasets)
        .map(v=>{
          const countryName = v.key;
          result[countryName] = {}
          v.values.forEach(bySite=> {
            const siteName = bySite.key;
            result[countryName][siteName] = {}
            bySite.values.forEach(byActivity => {
              const activityName = byActivity.key;
              result[countryName][siteName][activityName] = 0;
              byActivity.values.forEach(val=> {
                result[countryName][siteName][activityName] = val.nonCompliance;
              })
            })
            
          });
        delete v.key;
        delete v.values;
        return v
      })
    } else {
      result = null;
    }
    return result
  } catch (error) {
    console.error(`Error in getComplianceRiskScoreData ${processName}`)
    console.log(error)
    throw error;
  }

  
}

const getGeodemographicComplianceData = async (params)=> {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const studyComplianceModel = new StudyComplianceModel({clientId});
  try {
    const datasets = await studyComplianceModel.getGeodemographicCompliance(params);
    let result = {};

    if (datasets.length > 0) {
      // map object
      d3.nest()
        .key(d => d.countryName).sortKeys(d3.ascending)
        .key(d => d.siteName).sortKeys(d3.ascending)
        .entries(datasets)
        .map(v=>{
          const countryName = v.key;
          result[countryName] = {}
          v.values.forEach(bySite=> {
            const siteName = bySite.key;
            result[countryName][siteName] = {}
            bySite.values.forEach(val => {
              result[countryName][siteName].city = val.siteCity ? val.siteCity : countryName;
              result[countryName][siteName].state = val.siteState ? val.siteState : "";
              result[countryName][siteName].country = countryName;
              result[countryName][siteName].complianceRate = val.compliance_rate;
              result[countryName][siteName].nParticipant = val.nParticipant;
              result[countryName][siteName].totalParticipants = val.total_participants;
              result[countryName][siteName].participantPercentage = val.participantPercentage;
            })           
          });
        return v
      })
    } else {
      result = null;
    }
    return result
  } catch (error) {
    logger.createLog('', `Error in getGeodemographicComplianceData`, error);
    throw error;
  } 
}

const getComplianceChangeScoreData = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}-${params.activityName}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let result = {};
    let tempResult = {};
    const studyComplianceModel = new StudyComplianceModel({
      clientId
    });
    let complianceChangeData = null;
    let deviationData = null;
    // let dateData = {};
    [complianceChangeData, deviationData] = await Promise.all([
      studyComplianceModel.getComplianceChangeScore(params),
      studyComplianceModel.getComplianceDeviation(params)
    ])

    if (complianceChangeData.length <= 0 && deviationData.length <= 0) {
      return null; // handled in caller
    }

    if (complianceChangeData.length != deviationData.length) {
      console.error(`Error in getComplianceChangeScoreData ${processName}: change percentage and deviation is not having the same length`)
    }
    
    // map object
    complianceChangeData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .key(d => d.activityName).sortKeys(d3.ascending)
                  .key(d => d.date).sortKeys(d3.ascending)
                  .entries(complianceChangeData)
                  .map(v=>{
                    const countryName = v.key;
                    tempResult[countryName] = {}
                    v.values.forEach(bySite=> {
                      const siteName = bySite.key;
                      tempResult[countryName][siteName] = {}
                      bySite.values.forEach(byActivity => {
                        const activityName = byActivity.key;
                        tempResult[countryName][siteName][activityName] = {};
                        byActivity.values.forEach(byDate=> {
                          const dateActivity = byDate.key;
                          tempResult[countryName][siteName][activityName][dateActivity] = {}
                          byDate.values.forEach(val=> {
                            console.log(`val: ${JSON.stringify(val)}`)
                            // dateData.date = val.date;
                            tempResult[countryName][siteName][activityName][dateActivity].changePercentage = val.changePercentage;
                            // tempResult[countryName][siteName][activityName].push(dateData);
                            // dateData.deviation = val.standardDeviation;
                          })
                        })
                      })
                      
                    });
                  delete v.key;
                  delete v.values;
                  return v
                });
    
    deviationData = d3.nest()
                .key(d => d.countryName).sortKeys(d3.ascending)
                .key(d => d.siteName).sortKeys(d3.ascending)
                .key(d => d.activityName).sortKeys(d3.ascending)
                .key(d => d.date).sortKeys(d3.ascending)
                .entries(deviationData)
                .map(v=>{
                  const countryName = v.key;
                  result[countryName] = {}
                  //below to combine other query result
                  if (!tempResult[countryName]) {
                    tempResult[countryName] = {}
                  }
                  v.values.forEach(bySite=> {
                    const siteName = bySite.key;
                    result[countryName][siteName] = {}
                    if (!tempResult[countryName][siteName]) {
                      tempResult[countryName][siteName] = {}
                    }
                    bySite.values.forEach(byActivity=>{
                      const activityName = byActivity.key;
                      result[countryName][siteName][activityName] = []
                      if (!tempResult[countryName][siteName][activityName]) {
                        tempResult[countryName][siteName][activityName] = [];
                      }
                      byActivity.values.forEach(byDate=> {
                        const dateActivity = byDate.key;
                        if (!tempResult[countryName][siteName][activityName][dateActivity]) {
                          tempResult[countryName][siteName][activityName][dateActivity] = {};
                          tempResult[countryName][siteName][activityName][dateActivity].changePercentage = 0;
                        }
                        byDate.values.forEach(val=> {
                          console.log(`val: ${JSON.stringify(val)}`)
                          const dateData = {};
                          dateData.date = val.date;
                          dateData.deviation = val.standardDeviation;
                          dateData.changePercentage = tempResult[countryName][siteName][activityName][dateActivity].changePercentage;
                          result[countryName][siteName][activityName].push(dateData);
                        })
                      })
                    })
                  });
                  delete v.key;
                  delete v.values;
                  return v
                })

    return result;
  } catch (error) {
    console.error(`Error in getComplianceChangeScoreData ${processName}`)
    console.error(error)
    throw error;
  }
}

const getComplianceInsightScore = async (params)=> {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}-${params.activityName}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  try {
    let result = {};
    let sqlData = null;
    const studyComplianceModel = new StudyComplianceModel({
      clientId
    });
    const commonModel = new CommonModel({
      clientId
    });
    let previousComplianceData = null;
    if (!params.fromDate) {
      const [studyMetaData] = await commonModel.getStudyMetaData(params);
      params.fromDate = moment(studyMetaData.fromDate).format('YYYY-MM-DD');
      params.toDate = moment(studyMetaData.toDate).format('YYYY-MM-DD');
    }
    sqlData = await studyComplianceModel.getComplianceInsightScore(params);

    if (sqlData.length <= 0) {
      return null; // handled in caller
    }
    // map object
    sqlData = d3.nest()
                  .key(d => d.countryName).sortKeys(d3.ascending)
                  .key(d => d.siteName).sortKeys(d3.ascending)
                  .entries(sqlData)
                  .map(v=>{
                    const countryName = v.key;
                    result[countryName] = {}
                    v.values.forEach(bySite=> {
                      const siteName = bySite.key;
                      result[countryName][siteName] = {}
                      bySite.values.forEach(val => {
                        console.log(`val: ${JSON.stringify(val)}`)
                        // tempResult[countryName][siteName].currentCompliance = val.percentCompliance;
                        result[countryName][siteName].currentCompliance = Number(val.percent_compliance_now).toFixed(2);
                        result[countryName][siteName].lastWeekCompliance = Number(val.percent_compliance_previous7day).toFixed(2)
                      })
                      
                    });
                  delete v.key;
                  delete v.values;
                  return v
                });

    return result;
  } catch (error) {
    console.error(`Error in getComplianceInsightScore ${processName}`)
    console.error(error)
    throw error;
  }
}

const getNonCompliantActivities = async (params)=> {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const studyComplianceModel = new StudyComplianceModel({clientId});
  try {
    const datasets = await studyComplianceModel.getNonCompliantActivities(params);
    let result = {};

    if (datasets.length > 0) {
      // map object
      d3.nest()
        .key(d => d.countryName).sortKeys(d3.ascending)
        .key(d => d.siteName).sortKeys(d3.ascending)
        .key(d => d.activityName).sortKeys(d3.ascending)
        .entries(datasets)
        .map(v=>{
          const countryName = v.key;
          result[countryName] = {}
          v.values.forEach(bySite=> {
            const siteName = bySite.key;
            result[countryName][siteName] = {}
            bySite.values.forEach(byActivity => {
              const activityName = byActivity.key;
              result[countryName][siteName][activityName] = 0;
              byActivity.values.forEach(val=> {
                result[countryName][siteName][activityName] = val.nonCompliance;
              })
            })
            
          });
        delete v.key;
        delete v.values;
        return v
      })
    } else {
      result = null;
    }
    return result
  } catch (error) {
    console.error(`Error in getNonCompliantActivitiesData ${processName}`)
    console.log(error)
    throw error;
  }
}

module.exports = {
  getComplianceData,
  getComplianceDetailsData,
  getQueriesData,
  getActivitiesData,
  getCompletionData,
  getMissingData,
  getFormStatusData,
  getQueriesTimeData,
  getParticipantCompletionData,
  getComplianceRiskScoreData,
  getGeodemographicComplianceData,
  getComplianceChangeScoreData,
  getComplianceInsightScore,
  getNonCompliantActivities
}