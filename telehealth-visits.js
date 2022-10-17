'use strict';
const telehealthService = require('../../services/telehealth');
const CONST_VAL = require('../../config/constants');
const { AUTHORIZED_ROLES } = require('../../constants');
const utils = require('../../config/utils');
const helper = require('../../utils');
const { Joi, JoiDate } = require('../../packages');
const dateFormat = Joi.extend(JoiDate);

const eventBodySchema = Joi.object({
  fromDate: dateFormat.date().format('YYYY-MM-DD').optional(),
  toDate: dateFormat.date().format('YYYY-MM-DD').optional(),
  siteId: Joi.string().optional(),
  participantsIds: Joi.array().items(Joi.string()).optional(),
});

module.exports.handler = async (event) => {
  const eventBody = JSON.parse(event.body);
  try {
    if (!helper.validateUserRoles(event, AUTHORIZED_ROLES)) {
      const response = utils.error(
        CONST_VAL.statusCode.Forbidden,
        CONST_VAL.status.FORBIDDEN
      );
      return response;
    }

    if (!helper.isClientIdPresent(event)) {
      const response = utils.error(
        CONST_VAL.statusCode.BadRequest,
        CONST_VAL.status.BAD_REQUEST,
        CONST_VAL.status.CLIENT_ID_MSG
      );
      return response;
    }
    const validationResult = eventBodySchema.validate(eventBody);
    if (validationResult.error) {
      const response = utils.error(
        CONST_VAL.statusCode.BadRequest,
        CONST_VAL.status.BAD_REQUEST,
        validationResult.error.message
      );
      return response;
    }

    if (
      eventBody.toDate &&
      eventBody.fromDate &&
      !helper.isFromBeforeToDate(eventBody.toDate, eventBody.fromDate)
    ) {
      const response = utils.error(
        CONST_VAL.statusCode.BadRequest,
        CONST_VAL.status.BAD_REQUEST,
        CONST_VAL.status.DATE_DIFF_MSG
      );
      return response;
    }
  } catch (error) {
    const response = utils.error(CONST_VAL.statusCode.InternalServerError, CONST_VAL.status.WENT_WRONG)
    utils.createLog(
      '',
      `getTelehealthVisitsDuration request validation Error`,
      error
    );
    return response;
  }

  let response;
  let studyId = event.pathParameters.studyid;
  let clientId = event.headers.clientid ? event.headers.clientid : null;
  let siteId = eventBody ? eventBody.siteId : null;
  let fromDate = eventBody ? eventBody.fromDate : null;
  let toDate = eventBody ? eventBody.toDate : null;
  let participantsIds = eventBody ? eventBody.participantsIds : null;

  const params = {
    studyId,
    clientId,
    siteId,
    fromDate,
    toDate,
    participantsIds,
  };
  try {
    utils.createLog('', `getTelehealthVisitsDuration request params`, params);
    const resultData = await telehealthService.getTelehealthVisitsDuration(
      params
    );
    utils.createLog('', `getTelehealthVisitsDuration Query Data`, resultData);
    response = utils.success(
      CONST_VAL.statusCode.Succes,
      CONST_VAL.status.SUCCESS,
      resultData
    );
  } catch (error) {
    response = utils.error(CONST_VAL.statusCode.InternalServerError, CONST_VAL.status.WENT_WRONG)
    utils.createLog('', `getTelehealthVisitsDuration SQL query Error`, error);
  }
  return response;
};
