const PatientAnalyticsModel = require('../models/patientAnalytics');
const {d3, moment, lodash: _} = require('../packages')
const utils = require('../utils');
const s3Services = require('../utils/s3Services');
const CONST_VAL = require('../constants');
const Redis = require("../config/redis")
const HORIZONTAL_BARCHART = 'HorizontalBarChart';
const PIECHART = 'PieChart';
const WORDCLOUD = 'WordCloud';
const VERTICAL_BARCHART = 'VerticalBarChart';

const processVis = (questionType, chartType, data)=> {
  const result = [];
  if ([HORIZONTAL_BARCHART, PIECHART, WORDCLOUD].includes(chartType)) {
    const resultObj = {
      milestoneOrder: null,
      milestoneName: null,
      choosenAnswer: [],
      surveyData: []
    }
    data.forEach(byMilestone=> {
      resultObj.milestoneName = byMilestone.key;
      resultObj.milestoneOrder = byMilestone.values[0].milestoneOrder;
      byMilestone.values.forEach(d=> {
        const answerIdx = resultObj.surveyData.findIndex(s=> s.answer === d.choosenAnswer);
        if (answerIdx === -1) {
          resultObj.surveyData.push({answer: d.choosenAnswer, total: d.total});
        } else {
          resultObj.surveyData[answerIdx].total += d.total;
        };
        if (d.flag_me === 'Yes') {
          const flagMeIdx = resultObj.choosenAnswer.findIndex(s=> s === d.choosenAnswer);
          if (flagMeIdx === -1) {
            resultObj.choosenAnswer.push(d.choosenAnswer)
          }
        }
      });
    });
    if (resultObj.choosenAnswer.length > 0) {
      result.push(resultObj)
    }
    return result;
  } else if ([VERTICAL_BARCHART].includes(chartType)) {
    data.forEach(byMilestone=> {
      const resultObj = {
        milestoneOrder: null,
        milestoneName: null,
        choosenAnswer: [],
        surveyData: []
      }
      resultObj.milestoneName = byMilestone.key;
      resultObj.milestoneOrder = byMilestone.values[0].milestoneOrder;
      byMilestone.values.forEach(d=> {
        const answerIdx = resultObj.surveyData.findIndex(s=> s.answer === d.choosenAnswer);
        if (answerIdx === -1) {
          resultObj.surveyData.push({answer: d.choosenAnswer, total: d.total});
        } else {
          resultObj.surveyData[answerIdx].total += d.total;
        };
        if (d.flag_me === 'Yes') {
          resultObj.choosenAnswer.push(d.choosenAnswer);
        }
      });
      if (resultObj.choosenAnswer.length > 0) {
        result.push(resultObj)
      }
    });
    return result;
  }
}

const transformChartWordCloud = (data) => {
  const { surveyData, choosenAnswer } = data[0];

  // console.log("WordCloud Transform choosenAnswer : ", JSON.stringify(choosenAnswer));
  // console.log("WordCloud Transform surveyData : ", JSON.stringify(surveyData));
  let sortedSurverData = surveyData;

  // sort descending by total
  sortedSurverData.sort(function(a, b) {
    if (a.total < b.total) return 1;
    if (a.total > b.total) return -1;
    return 0;
  });

  // console.log("WordCloud Transform sorted surveyData : ", JSON.stringify(sortedSurverData));

  // get top ten
  let topSurveyData = sortedSurverData.slice(0, 10);
  
  // console.log("WordCloud Transform top 10 surveyData : ", JSON.stringify(topSurveyData));

  // check my answers are exist or not in top survey
  const myChoosenAnswerDontExistInTopSurvey = [];
  choosenAnswer.forEach((answer) => {
    const isExistInTopSurvey = topSurveyData.filter((row) => answer === row.answer).length;
    if (!isExistInTopSurvey) {
      myChoosenAnswerDontExistInTopSurvey.push(answer)
    }
  });

  // insert my answer to top survey
  if (myChoosenAnswerDontExistInTopSurvey.length) {
    const myChoosenAnswer = sortedSurverData.filter((row) => myChoosenAnswerDontExistInTopSurvey.includes(row.answer));
    topSurveyData = [
      ...topSurveyData,
      ...myChoosenAnswer,
    ]
  }

  // console.log("WordCloud Transform top 10 surveyData with my data : ", JSON.stringify(topSurveyData));

  // check if all data have same total answer
  const isAllTotalSame = topSurveyData.filter((row) => row.total === topSurveyData[0].total).length === topSurveyData.length;

  // console.log("WordCloud Transform top 10 surveyData is same total value : ", isAllTotalSame);
  
  if (isAllTotalSame) {
    // re-sort by alphabet ascending
    topSurveyData.sort(function(a, b) {
      if (a.answer < b.answer) return -1;
      if (a.answer > b.answer) return 1;
      return 0;
    });
  } else {
    // re-sort by total descending
    topSurveyData.sort(function(a, b) {
      if (a.total < b.total) return 1;
      if (a.total > b.total) return -1;
      return 0;
    });
  }

  // console.log("WordCloud Transform top 10 surveyData resorting : ", JSON.stringify(topSurveyData));

  return [
    {
      ...data[0],
      choosenAnswer,
      surveyData: topSurveyData,
    },
  ]
};

const transformByChartType = (chartType, data) => {
  if (data && data.length) {
    if (chartType === 'WordCloud') {
      return transformChartWordCloud(data);
    }
  }

  return data;
}

const getInsightData = async (params)=> {
  let {clientId, studyId, participantId} = params;
  const processName = `${params.clientId}-${params.studyId}-${params.participantId}`;
  let result = {
    participantName: null,
    surveys: []
  };
  let participantName = null;
  let s3ClientBucket = null;
  let questionsFromS3 = null;
  let forcePPMDStudy = false;
  params.forcePPMDStudy = forcePPMDStudy;
  try {
    const patientAnalyticstModel = new PatientAnalyticsModel({
      clientId
    });
    const dbPool = await patientAnalyticstModel._initDbConnectionPool(clientId, CONST_VAL.constants.DATABASE.RESEARCH_ANALYTICS_DB);
    const [data] = await dbPool.query(`select * from research.client_config cc inner join research.client c on cc.id = c.id 
                                        where cc.id = ${clientId}`)
    const clienConfig = data[0];
    s3ClientBucket = clienConfig.s3_bucket;
    if (!clienConfig) {
      throw new Error('Client config not found')
    }
    const hardcodeObjKey = `studies/${studyId}/hardcodeStudy.json`
    const AwsS3Obj = await s3Services.checkBucketExists(s3ClientBucket);
    if (!AwsS3Obj) {
      throw new Error('Client S3 bucket not found.');
    }
    // BEGIN. This is temporary code to force shows PPMD study
    const isHardCodeStudyObj = await s3Services.doesObjectExists(hardcodeObjKey, s3ClientBucket, clientId);
    if (isHardCodeStudyObj) {
      console.log('Fetching isHardCodeStudyObj Data - ', isHardCodeStudyObj);
      const hardcodeFile = await s3Services.readS3JSONObject(hardcodeObjKey, s3ClientBucket);
      const hardcodeObj = JSON.parse(hardcodeFile);
      if (hardcodeObj && hardcodeObj.hardcodeStudy) {
        params.studyId = hardcodeObj.studyId;
        params.participantId = hardcodeObj.participantId;
        params.forcePPMDStudy = true;
      }
    }
    // END. This is temporary code to force shows PPMD study

    const objectKey = `studies/${studyId}/surveys/questionsAnalytics.json`
    console.log('Checking if questionsAnalytics exists or not -', objectKey);
    const isActivityObj = await s3Services.doesObjectExists(objectKey, s3ClientBucket, clientId);
    if (isActivityObj) {
        console.log('Fetching questionsAnalytics Data - ', objectKey);
        const s3ConfigFile = await s3Services.readS3JSONObject(objectKey, s3ClientBucket);
        questionsFromS3 = JSON.parse(s3ConfigFile);
    }
    else
        throw new Error('Activity object not found.');

    const [participantData] = await dbPool.query(`SELECT * FROM research.participant_meta where pid = '${participantId}'`);
    if (participantData && participantData.length) {
      participantData.forEach((participant) => {
        if (participant.field_name === 'firstName') {
          participantName = participant.field_value;
        }
      });
    }
      const verticalBarchartQuestionIds=[];
      const questionIds=[];
      console.log("questionsFromS3="+questionsFromS3.questions.map(q=> q.questionId));
      console.log("ChartTypeFromS3="+questionsFromS3.questions.map(c=> c.chartType));
    questionsFromS3.questions.forEach((question)=>{ 
      if(question.chartType===VERTICAL_BARCHART){
        verticalBarchartQuestionIds.push(question.questionId);        
      }else{
        questionIds.push(question.questionId);
      }
    });
    console.log("verticalBarchartQuestionIds"+verticalBarchartQuestionIds);
    console.log("questionIds"+questionIds);
    params.verticalBarchartQuestionIds=verticalBarchartQuestionIds;
    params.questionIds=questionIds;
    //params.questionIds = questionsFromS3.questions.map(q=> q.questionId);
    const queryResult1 = await patientAnalyticstModel.getParticipantInsight(params);//old query result
    const queryResult2=await patientAnalyticstModel.getParticipantInsightVerticalBarchart(params);
    const queryResult=queryResult1.concat(queryResult2);
    console.log("querydata2:"+queryResult2.questionId);
    if (!queryResult || queryResult.length <= 0) {
      return result;
    }
    d3.nest()
    .key(survey => survey.questionId)
    .key(milestone => milestone.milestoneName).sortKeys(d3.ascending)
    .entries(queryResult)
    .map(arg => {
      const questionId = arg.key;
      const surveyQuestion = arg.values[0].values[0].surveyQuestion;
      const questionType = arg.values[0].values[0].questionType;
      const questionData = questionsFromS3.questions.find(q=> q.questionId === questionId);
      const analyticsObj = {
        surveyOrder: questionData.questionOrder,
        surveyQuestion,
        questionType,
        chartType: questionData.chartType,
        milestone: transformByChartType(
          questionData.chartType,
          processVis(questionType, questionData.chartType, arg.values)
        )
      }

      result.surveys.push(analyticsObj);
    });
    // comparing questionsAnalytics question with queryResult filter out missing question 
    let missingQuestionIds = questionsFromS3.questions.filter(f =>
      !queryResult.some(d => d.questionId == f.questionId)
    );
    console.log('missingQuestionIds Array ----------- ', missingQuestionIds);
    if (missingQuestionIds.length > 0){
      // surveyQuestion data finding for missing question
      result.surveys = await findMissingQuestionsData(missingQuestionIds, s3ClientBucket, studyId, clientId, objectKey, result.surveys)
    }
    result.surveys = _.orderBy(result.surveys, ['surveyOrder'], ['asc'])
    //sorting response on basis of availability of data
    result.surveys = _.sortBy(result.surveys, function(item) {
      return (item.milestone.length === 0) ? 1 : 0;
    });
    result.participantName = participantName;
    return result;
  } catch (error) {
    console.error(`Error in getInsightData ${processName}`)
    console.log(error)
    throw error;
  }
};

// surveyQuestion data finding for missing question
async function findMissingQuestionsData (missingQuestionIds, s3ClientBucket, studyId, clientId, objectKey, data) {
  const redis = new Redis();
  const redisClient = await redis._initRedisConnectionPool();
  try {
    for (const missingQuestionId of missingQuestionIds) {
      console.log(`missingQuestionIdData fetching`);
      // console.log(`initiate connection in service: ${JSON.stringify(redisClient)}`);
      let redisKey = `studies:${studyId}:surveyQuestions:${missingQuestionId.questionId}` ;
      let invalidQuestionKey = `studies:${studyId}:invalidQuestionsIds:${missingQuestionId.questionId}`
      console.log(`key: ${redisKey}`);
      let surveyQuestionData = await redis._getData(redisClient,redisKey);
      console.log(`surveyQuestionData: ${surveyQuestionData}`);
      if (!surveyQuestionData) {
        //Not found in redis so listing all files from s3
        let isInvalidQuestion = await redis._getData(redisClient,invalidQuestionKey);
        if (isInvalidQuestion){
          console.log(`ERROR: Question Id from Analytics found Invalid: ${missingQuestionId.questionId}`);
        } else {
          const distFolderPath = `studies/${studyId}/surveys/`
          let listFileFromS3 = await s3Services.listObjectFromS3(s3ClientBucket, distFolderPath)
          let completeSureveyData = []
          for (const file of listFileFromS3.Contents) {
            //reading all listed files and concat all question in Json Object
            console.log('Checking if surveyQuestions exists or not -', file.Key);
            const isSurveyObj = await s3Services.doesObjectExists(file.Key, s3ClientBucket, clientId);
            if (isSurveyObj && file.Key !== objectKey) {
              console.log('Fetching surveyQuestions Data - ', file.Key);
              const s3ConfigFile = await s3Services.readS3JSONObject(file.Key, s3ClientBucket);
              if (JSON.parse(s3ConfigFile).questions){
                completeSureveyData = completeSureveyData.concat(JSON.parse(s3ConfigFile).questions);
              }
            }
          }
          for(const questionData of completeSureveyData){
            // console.log('questionData------------------------------------ ', questionData)
            if (questionData.body){
              //all question storing to redis with seprate key with questionId
              let questionKey = `studies:${studyId}:surveyQuestions:${questionData.id}`
              let questionValue = JSON.stringify({
                surveyQuestion: questionData.body,
                questionType: questionData.type,
              })
              redisInsert = await redis._setData(redisClient, questionKey, questionValue);
              console.log(`result insert data: ${redisInsert}`);
            }
          }
          surveyQuestionData = await redis._getData(redisClient,redisKey);
            console.log(`surveyQuestionData after insert: ${surveyQuestionData}`);
          if(!surveyQuestionData){
            // founing question is not avilable in servery 
            redisInsertInvalid = await redis._setData(redisClient, invalidQuestionKey, 1);
            console.log(`ERROR: Question Id from Analytics added to Invalid list: ${missingQuestionId.questionId}`);
          }
        }
      }
      if (surveyQuestionData){
        // pushing missing question data to result
        surveyQuestionData = JSON.parse(surveyQuestionData)
        data.push({
          surveyOrder: missingQuestionId.questionOrder,
          surveyQuestion: surveyQuestionData.surveyQuestion,
          questionType: surveyQuestionData.questionType,
          chartType: missingQuestionId.chartType,
          milestone: []
        });
      }
    }
    redisClient.quit();
    return data
  }catch (err) {
  redisClient.quit();
  console.log('[Error in function findMissingQuestionsData]', err)
  throw err
  }
}

const getInsightDataTesting = async (params)=> {
  const {clientId, studyId, participantId} = params;
  let result = {};
  let surveyOrder = 1;
  const patientAnalyticstModel = new PatientAnalyticsModel({
    clientId
  });
  
  result.surveys = [
    {
      surveyOrder: 1,
      surveyQuestion: "Health Reason for Registration:",
      questionType: "SingleChoice",
      chartType: "HorizontalBarChart",
      milestone: [{
        milestoneOrder: 1,
        milestoneName: "Milestone 1",
        choosenAnswer: "Duchenne",
        surveyData: [{
            answer: "Becker",
            total: 20
          },
          {
            answer: "Confirmed carrier (I am a carrier of Becker or Duchenne and I do not have any symptoms)",
            total: 54
          },
          {
            answer: "Duchenne",
            total: 278
          },
          {
            answer: "Duchenne or Becker (not clear yet)",
            total: 17
          },
          {
            answer: "Manifesting carrier (I am a carrier of Duchenne or Becker 	and I have symptoms)",
            total: 35
          },
          {
            answer: "Not Affected",
            total: 5
          }
        ]
      }]
    },
    {
      surveyOrder: 2,
      surveyQuestion: "Have you had genetic testing?",
      questionType: "SingleChoice",
      chartType: "PieChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "Yes, genetic (DNA) test.",
        surveyData: [{
            answer: "I don't know.",
            total: 3
          },
          {
            answer: "Muscle biopsy only (no DNA test).",
            total: 6
          },
          {
            answer: "No.",
            total: 11
          },
          {
            answer: "Yes, genetic (DNA) test.",
            total: 240
          },
          {
            answer: "Yes, muscle biopsy and genetic (DNA) test.",
            total: 26
          },
        ]
      }]
    },
    {
      surveyOrder: 3,
      surveyQuestion: "Please choose the results as accurately as possible. Please choose one answer below.",
      questionType: "SingleChoice",
      chartType: "HorizontalBarChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "Deletion of one or more exons",
        surveyData: [{
            answer: "Deletion of one or more exons",
            total: 164
          },
          {
            answer: "Duplication of one or more exons",
            total: 28
          },
          {
            answer: "Frameshift deletion",
            total: 6
          },
          {
            answer: "Frameshift insertion AND deletion (ins/del)",
            total: 1
          },
          {
            answer: "I do not have the result, and I would like the Coordinator to help me get the result.",
            total: 8
          },
          {
            answer: "I do not have the result, but I do not need help at this time.",
            total: 6
          },
          {
            answer: "Missense mutation",
            total: 1
          },
          {
            answer: "No mutation detected (mutation unknown)",
            total: 6
          },
          {
            answer: "Nonsense mutation (premature stop codon)",
            total: 32
          },
          {
            answer: "Other mutation",
            total: 13
          },
        ]
      }]
    },
    {
      surveyOrder: 4,
      surveyQuestion: "Please write what is known about the specific mutation, and be as precise as possible. If the results are unclear, please leave this space blank. Here are some examples: deletion of exons 48-52, stop codon in exon 32",
      questionType: "SingleChoice",
      chartType: "WordCloud",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "46-48",
        surveyData: [
          {
            answer: "46-49",
            total: 1
          },
          {
            answer: "17-27",
            total: 1
          },
          {
            answer: "3-7",
            total: 1
          },
          {
            answer: "43-48",
            total: 1
          },
          {
            answer: "44",
            total: 1
          },
          {
            answer: "44-48",
            total: 2
          },
          {
            answer: "45",
            total: 3
          },
          {
            answer: "45-47",
            total: 1
          },
          {
            answer: "45-50",
            total: 2
          },
          {
            answer: "45-51",
            total: 1
          },
          {
            answer: "45-52",
            total: 1
          },
          {
            answer: "45-54",
            total: 2
          },
          {
            answer: "46",
            total: 1
          },
          {
            answer: "46-47",
            total: 1
          },
          {
            answer: "46-48",
            total: 1
          },
          {
            answer: "46-49",
            total: 1
          },
          {
            answer: "46-50",
            total: 1
          },
          {
            answer: "46-50 deletion",
            total: 2
          },
          {
            answer: "47-52",
            total: 1
          },
          {
            answer: "48",
            total: 1
          },
          {
            answer: "48-50",
            total: 1
          },
          {
            answer: "48-51",
            total: 1
          },
          {
            answer: "48-52",
            total: 3
          },
          {
            answer: "49-50",
            total: 1
          },
          {
            answer: "49-51",
            total: 1
          },
          {
            answer: "49,50",
            total: 1
          },
          {
            answer: "5-18",
            total: 1
          },
          {
            answer: "5-7",
            total: 1
          },
          {
            answer: "50-51",
            total: 1
          },
          {
            answer: "51-53",
            total: 1
          },
          {
            answer: "51-59",
            total: 1
          },
          {
            answer: "53 54 55",
            total: 1
          },
          {
            answer: "56",
            total: 1
          },
          {
            answer: "56-62",
            total: 1
          },
          {
            answer: "61  c.9100C>T",
            total: 1
          },
          {
            answer: "8-32 duplication",
            total: 1
          },
          {
            answer: "8-33",
            total: 1
          },
          {
            answer: "8-41 deletion",
            total: 1
          },
          {
            answer: "8-44",
            total: 1
          },
          {
            answer: "A deletion of exon 50 is predicted to be out of frame, which is consistent with a clinical diagnosis of Duchenne muscular dystrophy (DMD).",
            total: 1
          },
          {
            answer: "A splice site mutation IVS 32+5G>A",
            total: 1
          },
          {
            answer: "Becker’s - Point mutation of the dystrophin gene associated with absence or reduced dystrophin expression in cardiac muscle; brain and Purkinje cell dystrophin is upregulated ",
            total: 1
          },
          {
            answer: "C is supposed to be G. As far as I know my son and I are the only ones with this specific mutation",
            total: 1
          },
          {
            answer: "C. 1713:1 bp Duplication of T;codon:571 ",
            total: 1
          },
          {
            answer: "c.4471_4472del (p.Lys1491Glufs*19) ",
            total: 1
          },
          {
            answer: "Carrier",
            total: 1
          },
          {
            answer: "Carrier Positive for complex duplication of exons 5 through 29 and 42 through 43. MLPA revealed a duplication of exons 5 through 29 and exons 42 through 43",
            total: 1
          },
          {
            answer: "Del 49-50",
            total: 1
          },
          {
            answer: "delection of exons 46-51",
            total: 1
          },
          {
            answer: "Deleted 44-45",
            total: 1
          },
          {
            answer: "Deletion 18",
            total: 1
          },
          {
            answer: "deletion 40-43",
            total: 1
          },
          {
            answer: "Deletion 45-47",
            total: 1
          },
          {
            answer: "deletion 45-52",
            total: 1
          },
          {
            answer: "deletion 46-47",
            total: 1
          },
          {
            answer: "Deletion Exons 24 thru 45 inframe ",
            total: 1
          },
          {
            answer: "deletion exons 45-52",
            total: 1
          },
          {
            answer: "Deletion of DMD exons 45-52",
            total: 1
          },
          {
            answer: "Deletion of Econ’s 21-44",
            total: 1
          },
          {
            answer: "Deletion of Econ’s 48-54, exon skipping 55",
            total: 1
          },
          {
            answer: "Deletion of exon 48 in blood dna, muscle biopsy deletion Econ 49, both in frame",
            total: 1
          },
          {
            answer: "deletion of exon 45",
            total: 5
          },
          {
            answer: "Deletion of exon 48 in blood dna, muscle biopsy deletion Econ 49, both in frame",
            total: 1
          },
          {
            answer: "Duplication 8-11",
            total: 1
          },
          {
            answer: "Duplication exons 35-43",
            total: 1
          },
          {
            answer: "duplication of Exon 17",
            total: 1
          },
          {
            answer: "dystrophinopathy due to point mutation c.4057G>T in exon 29, which appears to result in a premature stop codon.",
            total: 1
          },
          {
            answer: "duplication of exons 2-12",
            total: 2
          },
          {
            answer: "Heterozygous for a deletion of Exxon’s 49-51 of the DMD gene",
            total: 1
          },
          {
            answer: "I am not sure if I have deletion you have a copy of my DNA test",
            total: 1
          },
          {
            answer: "i just no that i am a carrier of duchenne and my son is missing exons 17-24",
            total: 1
          },
          {
            answer: "Missense variation in Exon 11",
            total: 1
          },
          {
            answer: "stop codon in exon 34",
            total: 2
          },
          {
            answer: "Stop exon 62",
            total: 1
          },
          {
            answer: "unknown",
            total: 2
          },
        ]
      }]
    },
    {
      surveyOrder: 5,
      surveyQuestion: "How do you or your child (the person with Duchenne or Becker) usually get around? Please choose one answer below.",
      questionType: "SingleChoice",
      chartType: "HorizontalBarChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "I use a wheelchair or other mobility device and rarely or never walk. ",
        surveyData: [{
            answer: "I can get around on my own but I sometimes need help from a mobility device.",
            total: 50
          },
          {
            answer: "I use a wheelchair or other mobility device and rarely or never walk. ",
            total: 94
          },
          {
            answer: "I usually or always walk on my own without help or mobility devices.",
            total: 252
          },
          {
            answer: "My child is an infant/toddler and has not yet taken his/her first steps.",
            total: 16
          },
          {
            answer: "I can get around on my own but I sometimes need help from a mobility device.",
            total: 6
          }
        ]
      }]
    },
    {
      surveyOrder: 6,
      surveyQuestion: "Have you ever used corticosteroids? Please choose one answer below. ",
      questionType: "SingleChoice",
      chartType: "HorizontalBarChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "I am currently using Emflaza.",
        surveyData: [{
            answer: "I am currently using deflazacort.",
            total: 53
          },
          {
            answer: "I am currently using Emflaza.",
            total: 88
          },
          {
            answer: "I am currently using prednisone/prednisolone.",
            total: 68
          },
          {
            answer: "I don't know.",
            total: 8
          },
          {
            answer: "I have never used corticosteroids.",
            total: 167
          },
          {
            answer: "I used to take corticosteroids but I am not taking them anymore.",
            total: 34
          },
          {
            answer: "I have never used corticosteroids.",
            total: 5
          },
        ]
      }]
    },
    {
      surveyOrder: 7,
      surveyQuestion: "How old were you when you started corticosteroids?",
      questionType: "SingleChoice",
      chartType: "WordCloud",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "13",
        surveyData: [
          {
            answer: "1",
            total: 4
          },
          {
            answer: "10",
            total: 7
          },
          {
            answer: "11",
            total: 3
          },
          {
            answer: "12",
            total: 3
          },
          {
            answer: "13",
            total: 1
          },
          {
            answer: "16",
            total: 1
          },
          {
            answer: "19",
            total: 1
          },
          {
            answer: "2",
            total: 7
          },
          {
            answer: "25",
            total: 1
          },
          {
            answer: "3",
            total: 21
          },
          {
            answer: "4",
            total: 51
          },
          {
            answer: "5",
            total: 36
          },
          {
            answer: "6",
            total: 23
          },
          {
            answer: "7",
            total: 17
          },
          {
            answer: "8",
            total: 22
          },
          {
            answer: "54",
            total: 1
          },
          {
            answer: "80",
            total: 1
          }
        ]
      }]
    },
    {
      surveyOrder: 8,
      surveyQuestion: "Have you ever taken any heart medications? Please choose one answer below.",
      questionType: "SingleChoice",
      chartType: "PieChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "Yes",
        surveyData: [{
            answer: "I don't know",
            total: 2
          },
          {
            answer: "No",
            total: 229
          },
          {
            answer: "Yes",
            total: 146
          },
        ]
      }]
    },
    {
      surveyOrder: 9,
      surveyQuestion: "Have you had a lung function test (spirometry)? Please choose one answer below.",
      questionType: "SingleChoice",
      chartType: "PieChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "Yes",
        surveyData: [{
            answer: "I don't know",
            total: 79
          },
          {
            answer: "No",
            total: 202
          },
          {
            answer: "Yes",
            total: 76
          },
        ]
      }]
    },
    {
      surveyOrder: 10,
      surveyQuestion: "If a lung function test has been performed, please enter the FORCED VITAL CAPACITY (FVC) % predicted:",
      questionType: "SingleChoice",
      chartType: "VerticalBarChart",
      milestone: [
        {
        milestoneOrder: 0,
        milestoneName: "Milestone 1",
        choosenAnswer: "47",
        surveyData: [{
            answer: "100",
            total: 7
          },
          {
            answer: "50",
            total: 3
          },
          {
            answer: "47",
            total: 1
          },
          {
            answer: "90",
            total: 3
          }
        ]
        },
        {
          milestoneOrder: 1,
          milestoneName: "Milestone 2",
          choosenAnswer: "47",
          surveyData: [{
              answer: "100",
              total: 6
            },
            {
              answer: "50",
              total: 3
            },
            {
              answer: "47",
              total: 1
            },
            {
              answer: "90",
              total: 1
            }
          ]
        },
        {
          milestoneOrder: 2,
          milestoneName: "Milestone 3",
          choosenAnswer: "47",
          surveyData: [{
              answer: "100",
              total: 6
            },
            {
              answer: "50",
              total: 5
            },
            {
              answer: "47",
              total: 1
            },
            {
              answer: "90",
              total: 1
            }
          ]
        },
      ]
    },
    {
      surveyOrder: 11,
      surveyQuestion: "Have you ever had an x-ray to evaluate scoliosis/curvature of the spine? Please choose one answer below.",
      questionType: "SingleChoice",
      chartType: "PieChart",
      milestone: [{
        milestoneOrder: 3,
        milestoneName: "Milestone 4",
        choosenAnswer: "Yes",
        surveyData: [{
            answer: "I don't know",
            total: 27
          },
          {
            answer: "Yes",
            total: 76
          },
          {
            answer: "No, a spine x-ray was never offered and/or my doctor said that I don't need a spine x-ray.",
            total: 190
          },
        ]
      }]
    },
  ]
  return result;
};

module.exports = {
  getInsightData,
  getInsightDataTesting
}
