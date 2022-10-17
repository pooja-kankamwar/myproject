const TelehealthModel = require('../models/telehealth');
const { d3 } = require('../packages');
const utils = require('../utils');
const logger = require('../config/utils');

const getCancelledTelehealthVisits = async (params) => {
  let result = {};
  const processName = `${params.clientId}-${params.studyId}-${params.siteId}-${params.fromDate}-${params.toDate}`;
  const { clientId, siteId, studyId, fromDate, toDate } = params;
  try {
    let response = {};
    const telehealthModel = new TelehealthModel({ clientId });
    let cancelledTelehealthData =
      await telehealthModel.getCancelledTelehealthVisits(params);
    cancelledTelehealthData = d3
      .nest()
      .key((d) => d.country_name)
      .sortKeys(d3.ascending)
      .key((d) => d.siteName)
      .sortKeys(d3.ascending)
      .key((d) => d.categories)
      .sortKeys(d3.ascending)
      .entries(cancelledTelehealthData)
      .map((v) => {
        const country_name = v.key;
        result[country_name] = {};
        v.values.forEach((bySite) => {
          const siteName = bySite.key;
          result[country_name][siteName] = {};
          bySite.values.forEach((byHomehealth) => {
            const Homehealth = byHomehealth.key;
            result[country_name][siteName][Homehealth] = {};
            const homeHealthData = {};
            const [byHomehealthValues] = byHomehealth.values;
            homeHealthData.total_visit = byHomehealthValues.total_visit_count;
            homeHealthData.cancelled_count = byHomehealthValues.cancelled_count;
            result[country_name][siteName][Homehealth] = homeHealthData;
          });
          bySite.values.forEach((bySiteaDetail) => {
            const Site = bySiteaDetail.key;
            result[country_name][siteName][Site] = {};
            const siteaDetailData = {};
            const [bySiteaDetailValues] = bySiteaDetail.values;
            siteaDetailData.total_visit = bySiteaDetailValues.total_visit_count;
            siteaDetailData.cancelled_count = bySiteaDetailValues.canceled;
            result[country_name][siteName][Site] = siteaDetailData;
          });
          bySite.values.forEach((byParticipant) => {
            const Participant = byParticipant.key;
            result[country_name][siteName][Participant] = {};
            const participantData = {};
            const [byParticipantValues] = byParticipant.values;
            participantData.total_visit = byParticipantValues.total_visit_count;
            participantData.cancelled_count = byParticipantValues.canceled;
            result[country_name][siteName][Participant] = participantData;
          });
        });
        return true;
      });
    response.studyId = params.studyId;
    response.countries = result;
    return response;
  } catch (error) {
    logger.createLog('', `Error in getCancelledTelehealthVisits`, error);
    throw error;
  }
};

const getTelehealthVisitsDuration = async (params) => {
  const { clientId, studyId } = params;
  try {
    const telehealthModel = new TelehealthModel({ clientId });
    const datasets = await telehealthModel.getTelehealthVisitsDuration(params);
    const thisData = {};
    const AVG_UNIT_TIME_BY_WEEKS_IN_STUDIES = 'avgUnitTimeByMonthInStudies',
      AVG_UNIT_TIME_BY_WEEKS_IN_COUNTRIES = 'avgUnitTimeByMonthInCountries',
      LOCATIONS = 'locations';
    let unitTimeAverageInSites = {},
      unitTimeAverageInStudies = [],
      unitTimeAverageInCountries = {};

    if (datasets.length > 0) {
      for (let i = 0; i < datasets.length; i++) {
        let countryName = datasets[i].country_name;
        let siteName = datasets[i].site_name;
        let month = datasets[i].Month;
        let year = datasets[i].Year;
        let Monthly_Average =
          Number(datasets[i].Monthly_Average) > 0
            ? Number(datasets[i].Monthly_Average)
            : 0;
        let Monthly_Min =
          Number(datasets[i].Monthly_Min) > 0
            ? Number(datasets[i].Monthly_Min)
            : 0;
        let Monthly_Max =
          Number(datasets[i].Monthly_Max) > 0
            ? Number(datasets[i].Monthly_Max)
            : 0;
        let Monthly_total =
          Number(datasets[i].Monthly_total) > 0
            ? Number(datasets[i].Monthly_total)
            : 0;
        let Monthly_count =
          Number(datasets[i].Monthly_count) > 0
            ? Number(datasets[i].Monthly_count)
            : 0;
        let dateFromMonth = utils.extractDateFromMonthAndYear(year, month);
        let convertedDateFromWeek = dateFromMonth;
        let rangeUnitTime = utils.monthFormat(month, year);
        let indexUnitTime = unitTimeAverageInStudies.findIndex(
          (iut) => iut.x === rangeUnitTime
        );

        if (indexUnitTime > -1) {
          unitTimeAverageInStudies[indexUnitTime].y.push(Monthly_Average);
          unitTimeAverageInStudies[indexUnitTime].y2.push(Monthly_total);
          unitTimeAverageInStudies[indexUnitTime].ycount.push(Monthly_count);
          unitTimeAverageInStudies[indexUnitTime].ymin.push(Monthly_Min);
          unitTimeAverageInStudies[indexUnitTime].ymax.push(Monthly_Max);
        } else {
          unitTimeAverageInStudies.push({
            x: rangeUnitTime,
            y: [Monthly_Average],
            y2: [Monthly_total],
            ycount: [Monthly_count],
            ymax: [Monthly_Max],
            ymin: [Monthly_Min],
            rd: convertedDateFromWeek,
          });
        }

        if (countryName in unitTimeAverageInSites) {
          if (siteName in unitTimeAverageInSites[countryName]) {
            unitTimeAverageInSites[countryName][siteName] = [
              ...unitTimeAverageInSites[countryName][siteName],
              {
                x: rangeUnitTime,
                y: Monthly_Average,
                rd: convertedDateFromWeek,
              },
            ];
          } else {
            unitTimeAverageInSites[countryName][siteName] = [
              {
                x: rangeUnitTime,
                y: Monthly_Average,
                rd: convertedDateFromWeek,
              },
            ];
          }
        } else {
          unitTimeAverageInSites[countryName] = {
            [siteName]: [
              {
                x: rangeUnitTime,
                y: Monthly_Average,
                rd: convertedDateFromWeek,
              },
            ],
          };
        }

        if (countryName in unitTimeAverageInCountries) {
          let indexUnitTimeInCountries = unitTimeAverageInCountries[
            countryName
          ].findIndex((iutic) => iutic.x === rangeUnitTime);
          if (indexUnitTimeInCountries > -1) {
            unitTimeAverageInCountries[countryName][
              indexUnitTimeInCountries
            ].y.push(Monthly_Average);
            unitTimeAverageInCountries[countryName][
              indexUnitTimeInCountries
            ].y2.push(Monthly_total);
            unitTimeAverageInCountries[countryName][
              indexUnitTimeInCountries
            ].ycount.push(Monthly_count);
          } else {
            unitTimeAverageInCountries[countryName].push({
              x: rangeUnitTime,
              y: [Monthly_Average],
              ycount:[Monthly_count],
              y2: [Monthly_total],
              rd: convertedDateFromWeek,
            });
          }
        } else {
          unitTimeAverageInCountries[countryName] = [];
          unitTimeAverageInCountries[countryName].push({
            x: rangeUnitTime,
            y: [Monthly_Average],
            y2: [Monthly_total],
            ycount:[Monthly_count],
            rd: convertedDateFromWeek,
          });
        }
      }
      unitTimeAverageInStudies = unitTimeAverageInStudies
        .sort((a, b) => a.x - b.x)
        .map((item) => ({
          x: item.x,
          y: Number((utils.sum(item.y2)/utils.sum(item.ycount)).toFixed(2)),
          ymin: Math.min(...item.ymin),
          ymax : Math.max(...item.ymax),
          rd: item.rd,
        }));
      Object.keys(unitTimeAverageInCountries).map((element) => {
        unitTimeAverageInCountries[element] = unitTimeAverageInCountries[
          element
        ]
          .sort((a, b) => a.x - b.x)
          .map((elem) => ({
            x: elem.x,
            y: Number((utils.sum(elem.y2)/utils.sum(elem.ycount)).toFixed(2)),
            rd: elem.rd,
          }));
      });
      thisData[AVG_UNIT_TIME_BY_WEEKS_IN_STUDIES] = unitTimeAverageInStudies;
      thisData[AVG_UNIT_TIME_BY_WEEKS_IN_COUNTRIES] = unitTimeAverageInCountries;
      thisData[LOCATIONS] = unitTimeAverageInSites;
    } else {
      thisData[AVG_UNIT_TIME_BY_WEEKS_IN_STUDIES] = unitTimeAverageInStudies;
      thisData[AVG_UNIT_TIME_BY_WEEKS_IN_COUNTRIES] = unitTimeAverageInCountries;
      thisData[LOCATIONS] = unitTimeAverageInSites;
    }
    return thisData;
  } catch (err) {
    logger.createLog('', `Error in getTelehealthVisitsDuration`, err);
    throw err;
  }
};
module.exports = {
  getTelehealthVisitsDuration,
  getCancelledTelehealthVisits,
};
