const EnrollmentModel = require('../models/enrollment');
const {d3, moment, lodash: _} = require('../packages')
const utils = require('../utils');
const logger = require('../config/utils');

const getRecruitmentData = async (params)=> {
  const notAllowedKey = ['country_name', 'site_name', 'updated_date', 'study_id']
  let resultInJson = {}
  let locations = {}
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const enrollmentModel = new EnrollmentModel({
    clientId
  });
  const datasets = await enrollmentModel.getRecruitmentData(params);
  if (datasets.length > 0) {
    d3.nest()
    .key(country => country.country_name).sortKeys(d3.ascending)
    .key(site => site.site_name).sortKeys(d3.ascending)
    .entries(datasets)
    .map(arg => {
      let tmpData = {}
      let tmpSites = arg.values
      let countryName = arg.key

      for (let i = 0; i < tmpSites.length; i++) {
        let tmpStatuses = tmpSites[i].values
        let siteName = tmpSites[i].key

        for (let j = 0; j < tmpStatuses.length; j++) {
          let objStatus = tmpSites[i].values[j]

          Object.keys(objStatus).map(elem => {
            if (!notAllowedKey.includes(elem)) {
              tmpData[siteName] = tmpData[siteName]
                ? { ...tmpData[siteName], [elem]: objStatus[elem] }
                : { [elem]: objStatus[elem] }
            }
          })
        }
      }
      locations[countryName] = tmpData
    })

    resultInJson['studyId'] = datasets.length > 0 ? datasets[0].study_id : ''
    resultInJson['updated_date'] = datasets.length > 0 ? datasets[0].updated_date : ''
    resultInJson['locations'] = locations
  }
  return resultInJson;
}

const getEconsentData = async (clientId, studyId, siteId)=> {
  const notAllowedKey = ['country_name', 'site_name', 'updated_date', 'study_id']
  let resultInJson = {}
  let locations = {}
  const enrollmentModel = new EnrollmentModel({
    clientId
  });
  const datasets = await enrollmentModel.getConsentData(studyId, siteId);
  if (datasets.length > 0) {
    d3.nest()
    .key(country => country.country_name).sortKeys(d3.ascending)
    .key(site => site.site_name).sortKeys(d3.ascending)
    .entries(datasets)
    .map(arg => {
      let tmpData = {}
      let tmpSites = arg.values
      let countryName = arg.key
      
      for (let i = 0; i < tmpSites.length; i++) {
      let tmpStatuses = tmpSites[i].values
        let siteName = tmpSites[i].key

        for (let j = 0; j < tmpStatuses.length; j++) {
          let objStatus = tmpSites[i].values[j]

          Object.keys(objStatus).map(elem => {
            if (!notAllowedKey.includes(elem)) {
              tmpData[siteName] = tmpData[siteName]
                ? { ...tmpData[siteName], [elem]: objStatus[elem] }
                : { [elem]: objStatus[elem] }
            }
          })
        }
      }

      locations[countryName] = tmpData
    })

    resultInJson['studyId'] = datasets.length > 0 ? datasets[0].study_id : ''
    resultInJson['updated_date'] = datasets.length > 0 ? datasets[0].updated_date : ''
    resultInJson['locations'] = locations
  }
  return resultInJson;
}

const getQuizCompletion = async (params)=> {
  const notAllowedKey = ['country_name', 'site_name', 'updated_date', 'study_id']
  let resultInJson = {}
  let locations = {}
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const enrollmentModel = new EnrollmentModel({
    clientId
  });
  const datasets = await enrollmentModel.getQuizCompletionData(params);
  if (datasets.length > 0) {
    d3.nest()
    .key(country => country.country_name).sortKeys(d3.ascending)
    .key(site => site.site_name).sortKeys(d3.ascending)
    .entries(datasets)
    .map(arg => {
      let tmpData = {}
      let tmpSites = arg.values
      let countryName = arg.key

      for (let i = 0; i < tmpSites.length; i++) {
        let tmpStatuses = tmpSites[i].values
        let siteName = tmpSites[i].key

        for (let j = 0; j < tmpStatuses.length; j++) {
          let objStatus = tmpSites[i].values[j]

          Object.keys(objStatus).map(elem => {
            if (!notAllowedKey.includes(elem)) {
              tmpData[siteName] = tmpData[siteName]
                ? { ...tmpData[siteName], [elem]: objStatus[elem] }
                : { [elem]: objStatus[elem] }
            }
          })
        }
      }

      locations[countryName] = tmpData
    })

    resultInJson['studyId'] = datasets.length > 0 ? datasets[0].study_id : ''
    resultInJson['updated_date'] = datasets.length > 0 ? datasets[0].updated_date : ''
    resultInJson['locations'] = locations
  }
  return resultInJson;
}

const getParticipantStatusData = async (params)=> {
    params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
    params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
    const {clientId, siteId, studyId, fromDate, toDate} = params;
    const enrollmentModel = new EnrollmentModel({clientId});
    const datasets = await enrollmentModel.getParticipantStatusData(params);

    const resultInJson = {}

    // Let's make default constant variable key
    const STUDY_ID_RESPONSE = 'studyId'
    const LOCATION_REPONSE_KEY = 'locations'
    const TOP_ACTIVE_PARTICIPANT_STATUS = 'topActiveParticipantStatus'

    let locations = {}
    let result = {};
    let tempResult = {}
    let topActiveParticipant = []

    if (datasets.length > 0) {
      // map object
      locations = d3.nest()
        .key(d => d.countryName).sortKeys(d3.ascending)
        .key(d => d.siteName).sortKeys(d3.ascending)
        .key(d => d.status).sortKeys(d3.ascending)
        .entries(datasets)
        .map(v=>{
          const countryName = v.key;
          result[countryName] = {}
          tempResult[countryName] = {}
          v.values.forEach(bySite=> {
            const siteName = bySite.key;
            result[countryName][siteName] = {}
            tempResult[countryName][siteName] = {}
            let totalStatus = 0
            let active = 0
            bySite.values.forEach(byStatus => {
              const statusName = byStatus.key;
              result[countryName][siteName].siteDetails = {};
              result[countryName][siteName].siteStatus = {};
              tempResult[countryName][siteName][statusName] = 0;
              result[countryName][siteName].siteStatus[statusName] = 0;
              byStatus.values.forEach(val=> {
                result[countryName][siteName].siteDetails.siteAddress = val.siteAddress;
                result[countryName][siteName].siteDetails.siteCity = val.siteCity;
                result[countryName][siteName].siteDetails.siteCountry = countryName;
                result[countryName][siteName].siteDetails.postalCode = val.postalCode;
                result[countryName][siteName].siteDetails.siteState = val.siteState;
                tempResult[countryName][siteName][statusName] += val.n_participant;
                totalStatus += val.n_participant
                if (statusName == 'ACTIVE'){
                  active = val.n_participant
                }
              })
              result[countryName][siteName].siteStatus = tempResult[countryName][siteName];
            })

            // populate data topActiveParticipants
            topActiveParticipant.push({
              site: siteName,
              country: countryName,
              totalActiveParticipant: active,
              totalParticipant: totalStatus
            })
            
          });
        delete v.key;
        delete v.values;
        return v
      })

      topActiveParticipant = topActiveParticipant.sort((a, b) => b.totalActiveParticipant - a.totalActiveParticipant)
    }

    resultInJson[STUDY_ID_RESPONSE] = studyId
    resultInJson[TOP_ACTIVE_PARTICIPANT_STATUS] = topActiveParticipant
    resultInJson[LOCATION_REPONSE_KEY] = result

    return resultInJson
}

const getScreenFailuresReasons = async (params)=> {
  params.fromDate = params.fromDate ? `${params.fromDate} 00:00:00` : null;
  params.toDate = params.toDate ? `${params.toDate} 23:59:59` : null;
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const enrollmentModel = new EnrollmentModel({clientId});
  const datasets = await enrollmentModel.getScreenFailuresReasons(params);

  // let locations = {}
  let result = {};

  if (datasets.length > 0) {
    // map object
    d3.nest()
      .key(d => d.countryName).sortKeys(d3.ascending)
      .key(d => d.siteName).sortKeys(d3.ascending)
      .key(d => d.statusChangeReason).sortKeys(d3.ascending)
      .entries(datasets)
      .map(v=>{
        const countryName = v.key;
        result[countryName] = {}
        v.values.forEach(bySite=> {
          const siteName = bySite.key;
          result[countryName][siteName] = {}
          bySite.values.forEach(byStatus => {
            const statusName = byStatus.key;
            result[countryName][siteName][statusName] = 0;
            byStatus.values.forEach(val=> {
              result[countryName][siteName][statusName] += val.n_participant;
            })
          })
          
        });
      delete v.key;
      delete v.values;
      return v
    })
  } else {
    result = {};
  }

  return result
}

const getWithdrawalReasons = async (clientId, studyId) => {
  const notAllowedKey = ['country_name', 'site_name', 'updated_date', 'study_id', 'total_withdrawal_reasons']
  const reasonValuePrefix = 'total_withdrawal_reasons'

  let resultInJson = {}
  let locations = {}

  const enrollmentModel = new EnrollmentModel({
    clientId
  })

  const datasets = await enrollmentModel.getWithdrawalReasons(studyId)

  if (datasets.length > 0) {
    d3
      .nest()
      .key(country => country.country_name).sortKeys(d3.ascending)
      .key(site => site.site_name).sortKeys(d3.ascending)
      .entries(datasets)
      .map(arg => {
        let tmpData = {}
        let tmpSites = arg.values
        let countryName = arg.key

        for (let i = 0; i < tmpSites.length; i++) {
          let tmpStatus = tmpSites[i].values
          let siteName = tmpSites[i].key

          for (let j = 0; j < tmpStatus.length; j++) {
            let objStatus = tmpSites[i].values[j]

            Object.keys(objStatus).map(elem => {
              if (!notAllowedKey.includes(elem)) {
                if (elem === 'withdrawal_reasons') {
                  let reasonsKey = objStatus[elem].includes(';') ? objStatus[elem].split(';') : objStatus[elem]
                  let reasonsValue = objStatus[reasonValuePrefix].includes(';') ? objStatus[reasonValuePrefix].split(';') : objStatus[reasonValuePrefix]

                  if (!Array.isArray) {
                    tmpData[siteName] = tmpData[siteName]
                      ? { ...tmpData[siteName], [reasonsKey]: parseInt(reasonsValue) }
                      : { [reasonsKey]: reasonsValue }
                  } else {
                    reasonsKey.forEach((reasonKey, idx) => {
                      tmpData[siteName] = tmpData[siteName]
                        ? { ...tmpData[siteName], [reasonKey]: parseInt(reasonsValue[idx]) }
                        : { [reasonKey]: parseInt(reasonsValue[idx]) }
                    })
                  }
                }
                tmpData[siteName] = tmpData[siteName]
                  ? { ...tmpData[siteName], [elem]:  objStatus[elem]}
                  : { [elem]: objStatus[elem] }
              }
            })
          }
        }

        locations[countryName] = tmpData
      })

    resultInJson['studyId'] = datasets.length > 0 ? datasets[0].study_id : ''
    resultInJson['updated_date'] = datasets.length > 0 ? datasets[0].updated_date : ''
    resultInJson['locations'] = locations
  }

  return resultInJson;
}

const getParticipantProgressData = async (params)=> {
  const {clientId, studyId, participantIds, siteId, fromDate, toDate} = params;
  const processName = `${clientId}-${studyId}`
  try {
    let result = {};
    let locations = {};
    const enrollmentModel = new EnrollmentModel({
      clientId
    });
    
    result['locations'] = {};
    let listActivities = await enrollmentModel.getAllStudyActivities(params);
    if (!listActivities || listActivities.length <= 0) {
      return result;
    }

    let activityStartOrder = (params.activityPage-1)*params.activityLimit;
    listActivities = listActivities.map(a=> {
      a.activityOrder = activityStartOrder;
      activityStartOrder++;
      return a;
    })

    params.listTaskInstanceIds = listActivities.map(a=> a.task_instance_id);
    
    const includedParticipants = [];
    const participants = await enrollmentModel.getParticipants(params);
    params.listParticipantIds = participants.map(a=> a.id);
    if (!participants || participants.length <= 0) {
      return result;
    }
    const datasets = await enrollmentModel.getParticipantProgress(params);
    if (datasets.length > 0) {
      const listParticipantData = d3.nest()
        .key(d => d.countryName).sortKeys(d3.ascending)
        .key(d => d.siteName).sortKeys(d3.ascending)
        .key(d => d.participantId).sortKeys(d3.ascending)
        .entries(datasets)
        .map(byCountry=>{
          const countryName = byCountry.key;
          locations[countryName] = {};
          byCountry.values.forEach(bySite => {
            const siteName = bySite.key;
            locations[countryName][siteName] = [];
            bySite.values.forEach(byParticipant => {
              const participantId = byParticipant.key;
              includedParticipants.push(participantId)
              const participantInfo = {};
              participantInfo.participantId = participantId;
              participantInfo.activities = [];
              listActivities.forEach(a=> {
                const participantStartDay = byParticipant.values.find(pv=>a.start_day< pv.participant_end_day);
                if(participantStartDay)
                {
                const activity = {};
                activity.activityName = a.task_title;
                activity.activityOrder = a.activityOrder;
                activity.activityProgress = `${a.current_counter}/${parseInt(a.max_counter)}`;
                activity.scheduledDays = a.start_day;
                activity.activityStatus = 'Not Scheduled';

                const participantActivity = byParticipant.values.find(pv=> pv.task_instance_id === a.task_instance_id);
                if (participantActivity) {
                  activity.activityStatus = participantActivity.status;
                } else {
                  activity.activityStatus = 'Not Scheduled';
                }
                participantInfo.activities.push(activity);
              }
              })
              locations[countryName][siteName].push(participantInfo);
            })

          });
          delete byCountry.key
          delete byCountry.values
          return byCountry;
        })

        participants.forEach(participant => {
          const countryName = participant.countryName;
          const siteName = participant.siteName;
          if (!includedParticipants.includes(participant.id)){
            const participantInfo = {};
            participantInfo.participantId = participant.id;
            participantInfo.activities = [];
            listActivities.forEach(a=> {
              if(a.start_day<participant.participant_end_day)
              {
              const activity = {};
              activity.activityName = a.task_title;
              activity.activityOrder = a.activityOrder;
              activity.activityProgress = `${a.current_counter}/${parseInt(a.max_counter)}`;
              activity.scheduledDays = a.start_day;
              activity.activityStatus = 'Not Scheduled';              
              participantInfo.activities.push(activity);
            }
            })

            if(!locations[countryName]){
              locations[countryName] = {
                [siteName]:[]
              }
            }
            
            if(!locations[countryName][siteName]){
              locations[countryName][siteName] = []
            }

            locations[countryName][siteName].push(participantInfo);
        }
        })
      result['locations'] = locations;
      
    }
    return result;
  } catch (error) {
    console.error(`Error in getParticipantProgressData ${processName}`)
    console.log(error)
    throw error;
  }
}

function removeA(array, text) {
  _.remove(array, function (arr) {
    return arr === text
  });
}

const getOverallEnrollment = async (params) => {
  const {clientId, studyId} = params;
    try {
        const enrollmentModel = new EnrollmentModel({ clientId });
        const data = await enrollmentModel.getOverallEnrollment(params);
        const datasets = data.overallSitesData
        const sitesStatusChangeData = data.siteStatusChangeData
        const thisData = {};

        // Let's make default constant variable key
        const AVG_UNIT_TIME_BY_WEEKS_IN_STUDIES = 'avgUnitTimeByMonthInStudies'
        const AVG_UNIT_TIME_BY_WEEKS_IN_COUNTRIES = 'avgUnitTimeByMonthInCountries'
        const LOCATIONS = 'locations'
        const TOP_FIVE_COUNTRIES = 'topFiveCountries'
        const TOP_FIVE_SITES = 'topFiveSites'
        const TOTAL_PARTICIPANTS = 'totalParicipants'
        const SITES_STATUS_LOG = 'sitesStatusChangeData'

        let eCountry = {}
        let topFiveCountry = {}
        let topFiveSite = {}
        let totalParticipant = []
        let sortableCountry = []
        let sortableSite = []

        // We will generating average base on week and years
        let unitTimeAverageInStudies = [] // variable to hold global range unit time -> W 16/2020 : [2, 3, 4]
        let unitTimeAverageInCountries = {}

        if (datasets.length > 0) {
            for (let i = 0; i < sitesStatusChangeData.length; i++){
              let inMonth = sitesStatusChangeData[i].month
              let inYear = sitesStatusChangeData[i].year
              let dateformat = utils.monthFormat(inMonth, inYear)
              sitesStatusChangeData[i].formatedMonth = dateformat
            }
            for (let i = 0; i < datasets.length; i++) {
                let countryName = datasets[i].country_name
                let siteName = datasets[i].site_name
                let month = datasets[i].month
                let year = datasets[i].year
                let participant = datasets[i].n_participant

                // let dateExtractedFromWeeks = utils.extractDateFromWeekAndYears(week, year)
                // let dateIntervalOneWeek = utils.addDate(dateExtractedFromWeeks, 7)
                let dateFromMonth = utils.extractDateFromMonthAndYear(year, month)
                let convertedDateFromWeek = dateFromMonth

                let rangeUnitTime = utils.monthFormat(month, year)
                let indexUnitTime = unitTimeAverageInStudies.findIndex(iut => iut.x === rangeUnitTime)

                if (indexUnitTime > -1) {
                    unitTimeAverageInStudies[indexUnitTime].y.push(participant)
                } else {
                    unitTimeAverageInStudies.push({ x: rangeUnitTime, y: [participant], rd: convertedDateFromWeek })
                }

                // Get total participant
                totalParticipant.push(participant)

                // Get Top 5 Country
                topFiveCountry[countryName] = topFiveCountry[countryName] ? [...topFiveCountry[countryName], participant] : [participant]

                // Get Top 5 Sites
                topFiveSite[siteName] = topFiveSite[siteName] ? [...topFiveSite[siteName], participant] : [participant]

                // Get value participant each sites
                if (countryName in eCountry) {
                    if (siteName in eCountry[countryName]) {
                        eCountry[countryName][siteName] = [ ...eCountry[countryName][siteName], { x: rangeUnitTime, y: Number(participant) > 0 ? Number(participant) : null, rd: convertedDateFromWeek } ]
                    } else {
                        eCountry[countryName][siteName] = [ { x: rangeUnitTime, y: Number(participant) > 0 ? Number(participant) : null, rd: convertedDateFromWeek } ]
                    }
                } else {
                    eCountry[countryName] = {
                        [siteName]: [{ x: rangeUnitTime, y: Number(participant) > 0 ? Number(participant) : null, rd: convertedDateFromWeek }]
                    }
                }

                if (countryName in unitTimeAverageInCountries) {
                    let indexUnitTimeInCountries = unitTimeAverageInCountries[countryName].findIndex(iutic => iutic.x === rangeUnitTime)

                    if (indexUnitTimeInCountries > -1) {
                        unitTimeAverageInCountries[countryName][indexUnitTimeInCountries].y.push(participant)
                    } else {
                        unitTimeAverageInCountries[countryName].push({ x: rangeUnitTime, y: [participant], rd: convertedDateFromWeek })
                    }

                } else {
                    unitTimeAverageInCountries[countryName] = []
                    unitTimeAverageInCountries[countryName].push({ x: rangeUnitTime, y: [participant], rd: convertedDateFromWeek })
                }
            }

            // Get total participant
            totalParticipant = utils.sum(totalParticipant)

            for (const item in topFiveCountry) {
                topFiveCountry[item] = utils.sum(topFiveCountry[item])
            }

            for (const item in topFiveSite) {
                topFiveSite[item] = utils.sum(topFiveSite[item])
            }

            for (const item in topFiveCountry) {
                sortableCountry.push([item, topFiveCountry[item]])
            }

            for (const item in topFiveSite) {
                sortableSite.push([item, topFiveSite[item]])
            }

            sortableCountry = sortableCountry.sort((a, b) => b[1] - a[1])
            sortableSite = sortableSite.sort((a, b) => b[1] - a[1])

            sortableCountry = sortableCountry.map(item => ({
                countryName: item[0],
                numParticipant: item[1],
                pctParticipant: (item[1] / datasets.length) * 100
            }))

            sortableSite = sortableSite.map(item => ({
                siteName: item[0],
                numParticipant: item[1],
                pctParticipant: (item[1] / datasets.length) * 100
            }))

            // find average base on study
            unitTimeAverageInStudies = unitTimeAverageInStudies.sort((a,b) => a.x - b.x).map(item => ({ x: item.x, y: utils.sum(item.y) / item.y.length, y2: utils.sum(item.y), rd: item.rd }))
            // Total Enrollment for country
            Object.keys(unitTimeAverageInCountries).map(element => {
                unitTimeAverageInCountries[element] = unitTimeAverageInCountries[element].map(elem => ({ x: elem.x, y: utils.sum(elem.y), rd: elem.rd }))
            })

            thisData[TOTAL_PARTICIPANTS] = totalParticipant
            thisData[AVG_UNIT_TIME_BY_WEEKS_IN_STUDIES] = unitTimeAverageInStudies
            thisData[AVG_UNIT_TIME_BY_WEEKS_IN_COUNTRIES] = unitTimeAverageInCountries
            thisData[TOP_FIVE_COUNTRIES] = sortableCountry.slice(0, 5)
            thisData[TOP_FIVE_SITES] = sortableSite.slice(0, 5)
            thisData[LOCATIONS] = eCountry
            thisData[SITES_STATUS_LOG] = sitesStatusChangeData
        } else {
            thisData[TOTAL_PARTICIPANTS] = totalParticipant
            thisData[AVG_UNIT_TIME_BY_WEEKS_IN_STUDIES] = unitTimeAverageInStudies
            thisData[AVG_UNIT_TIME_BY_WEEKS_IN_COUNTRIES] = unitTimeAverageInCountries
            thisData[TOP_FIVE_COUNTRIES] = sortableCountry
            thisData[TOP_FIVE_SITES] = sortableSite
            thisData[LOCATIONS] = eCountry
            thisData[SITES_STATUS_LOG] = sitesStatusChangeData
        }


        return thisData;
    } catch (err) {
        console.error(`Error in getOverallEnrollment`)
        console.log(err)
        throw err;
    }
}

const getNoncomplianceParticipantRetentionRate = async (params)=> {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const enrollmentModel = new EnrollmentModel({clientId});
  try {
    const datasets = await enrollmentModel.getNoncomplianceParticipantRetentionRate(params);
    let result = {};
        
    if (datasets.length > 0) {
      // map object
      d3.nest()
        .key(d => d.country_name).sortKeys(d3.ascending)
        .key(d => d.site_name).sortKeys(d3.ascending)
        .entries(datasets)
        .map(v=>{
          const country_name = v.key;
          result[country_name] = {}
          v.values.forEach(bySite=> {
            const site_name = bySite.key;
            result[country_name][site_name] = []
              bySite.values.forEach((val,i)=> {
               persentageData= {}
               persentageData.participant_id =val.participant_id
               persentageData.Non_Compliance =val.Non_Compliance
               persentageData.participantRetentionRate =val.participantRetentionRate
               result[country_name][site_name][i] = persentageData
              })
          });
        return true
      })
    } 
     return result
  } catch (error) {
    console.error(`Error in getNoncomplianceParticipantRetentionRate ${processName}`)
    logger.createLog('', `Error in getNoncomplianceParticipantRetentionRate`, error);
    console.log(error)
    throw error;
  }
}

const getTopReasonsForScreenFailures = async (params)=> {
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`
  const {clientId, siteId, studyId, fromDate, toDate} = params;
  const enrollmentModel = new EnrollmentModel({clientId});
  try {
    const datasets = await enrollmentModel.getTopReasonsForScreenFailures(params);
    let result = {};
        
    if (datasets.length > 0) {
    // map object
    d3.nest()
      .key(d => d.countryName).sortKeys(d3.ascending)
      .key(d => d.siteName).sortKeys(d3.ascending)
      .key(d => d.statusChangeReason).sortKeys(d3.ascending)
      .entries(datasets)
      .map(v=>{
        const countryName = v.key;
        result[countryName] = {}
        v.values.forEach(bySite=> {
          const siteName = bySite.key;
          result[countryName][siteName] = {}
          bySite.values.forEach(byStatus => {
            const statusName = byStatus.key;
            result[countryName][siteName][statusName] = 0;
            byStatus.values.forEach(val=> {
              result[countryName][siteName][statusName] += val.totalCount;
            })
          })
          
        });
      delete v.key;
      delete v.values;
      return v
    })
    } else {
      result = {};
    } 
     return result
  } catch (error) {
    console.error(`Error in getTopReasonsForScreenFailures ${processName}`)
    logger.createLog('', `Error in getTopReasonsForScreenFailures`, error);
    console.log(error)
    throw error;
  }
}

module.exports = {
  getRecruitmentData,
  getEconsentData,
  getParticipantStatusData,
  getScreenFailuresReasons,
  getWithdrawalReasons,
  getParticipantProgressData,
  getQuizCompletion,
  getOverallEnrollment,
  getNoncomplianceParticipantRetentionRate,
  getTopReasonsForScreenFailures
}
