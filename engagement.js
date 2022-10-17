const EngagementModel = require('../models/engagement');
const {d3, moment} = require('../packages');
const _ = require('lodash');
const utils = require('../utils');

const getPlatformInsight = async (params)=> {
    const {clientId, siteId, studyId, fromDate, toDate, participantsIds} = params;
    const engagementModel = new EngagementModel({clientId});
    const dataDevice = await engagementModel.getDeviceUsage(params)
    if (dataDevice && dataDevice.length <= 0) {
      return null;
    }
    params.participantsIds = dataDevice.map(row => row.participant_id);
    const dataPlatform = await engagementModel.getPlatformInsight(params);
    const mapDevice = {android: 'Android Phone', ios: 'IOS Phone'};
    let result = {};
    
    if (dataPlatform.length > 0) {
        // aggregate data
        let datasets = [];

        for(let i=0; i<dataPlatform.length; i++) {
            datasets.push({
            ...dataPlatform[i], 
            ...(dataDevice.find((itmInner) => itmInner.participant_id === dataPlatform[i].participant_id))}
            );
        }
        
        // map object
        d3.nest()
            .key(d => d.countryName).sortKeys(d3.ascending)
            .key(d => d.siteName).sortKeys(d3.ascending)
            .key(d => d.status).sortKeys(d3.ascending)
            .key(d => d.device_os).sortKeys(d3.descending)
            .entries(datasets)
            .map(v=>{
            const countryName = v.key;
            result[countryName] = {};
            v.values.forEach(bySite=> {
                const siteName = bySite.key;
                result[countryName][siteName] = {};
                bySite.values.forEach(byStatus => {
                    const statusName = _.capitalize(byStatus.key);
                    result[countryName][siteName][statusName] = {};
                    byStatus.values.forEach(byOS=> {
                        let deviceOS = mapDevice[byOS.key];
                        if (!deviceOS) {
                          deviceOS = _.startCase(_.toLower(byOS.key));
                        }
                        result[countryName][siteName][statusName][deviceOS] = {};
                        result[countryName][siteName][statusName][deviceOS].n_devices = byOS.values.length;
                        let totalDuration = 0;
                        byOS.values.forEach(val => {
                            totalDuration += parseInt(val.session_duration);
                        })
                        result[countryName][siteName][statusName][deviceOS].avg_session = (totalDuration/byOS.values.length)/60;
                    })
                })
                
            });
            delete v.key;
            delete v.values;
            return v
        });
    } else {
        result = null;
    }
  
    return result
  }

  module.exports = {
    getPlatformInsight
  }