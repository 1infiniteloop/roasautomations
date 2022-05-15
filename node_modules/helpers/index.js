const util = require("util");
const {
    flip,
    curry,
    pipe,
    ifElse,
    always,
    reject,
    anyPass,
    map,
    when,
    is,
    split,
    fromPairs,
    identity,
    prop,
    values,
    reduceRight,
    mergeDeepRight,
} = require("ramda");
const {
    map: lodashmap,
    sumBy: lodashSumBy,
    groupBy: lodashGroupBy,
    filter: lodashFilter,
    uniqBy: lodashUniqBy,
    reduce: lodashReduce,
    keyBy: lodashKeyBy,
    has: lodashhas,
    sortBy: lodashsortby,
    get: lodashget,
    hasIn: lodashhasin,
    isNil,
    isEmpty,
    chunk: lodashchunk,
    toNumber,
    orderBy: lodashorderby,
    omitBy: lodashomitby,
} = require("lodash");
const validator = require("email-validator");
const { isIPv4, isIPv6 } = require("net");
const { mod, all } = require("shades");
const moment = require("moment");

const isNullString = (string) => string == "null";
const clean = (o) => pipe(ifElse(isNil, always(null), pipe(reject(anyPass([isNil, isEmpty, isNullString])), map(when(is(Object), clean)))))(o);
const loget = (path, obj) => lodashget(obj, path);
const lohasin = flip(lodashhasin);

const merge_by_key = (key, left, right) =>
    pipe(logroupby(prop(key)), values, map(reduceRight((prev, curr) => mergeDeepRight(prev, curr), {})))([...left, ...right]);

function getDates(startDate, endDate) {
    const dates = [];
    let currentDate = startDate;
    const addDays = function (days) {
        const date = new Date(this.valueOf());
        date.setDate(date.getDate() + days);
        return date;
    };
    while (currentDate <= endDate) {
        dates.push(currentDate);
        currentDate = addDays.call(currentDate, 1);
    }
    return dates;
}

const get_dates_range_array = (since, until) => {
    let since_date = pipe(
        split("-"),
        map(toNumber),
        mod(1)((value) => value - 1),
        (value) => new Date(...value)
    )(since);

    let until_date = pipe(
        split("-"),
        map(toNumber),
        mod(1)((value) => value - 1),
        (value) => new Date(...value)
    )(until);

    const dates = pipe(
        ([since_date, until_date]) => getDates(since_date, until_date),
        mod(all)((date) => moment(date, "YYYY-MM-DD").format("YYYY-MM-DD"))
    )([since_date, until_date]);

    return dates;
};

const removeFirstCharacterFromString = (string) => string.substring(1);
const urlToParams = split("&");
const paramsToPairs = map(split("="));
const urlToSearchParams = pipe(removeFirstCharacterFromString, urlToParams, paramsToPairs, fromPairs);
const toSearchParams = ifElse(isEmpty, identity, urlToSearchParams);
const lomap = flip(lodashmap);
const lohas = flip(lodashhas);
const losumby = flip(lodashSumBy);
const logroupby = flip(lodashGroupBy);
const lofilter = flip(lodashFilter);
const louniqby = flip(lodashUniqBy);
const loreduce = curry((reducerFn, data) => lodashReduce(data, reducerFn));
const lokeyby = flip(lodashKeyBy);
const losortby = flip(lodashsortby);
const lochunk = curry((size, data) => lodashchunk(data, size));
const loorderby = curry((path, direction, data) => lodashorderby(data, path, direction));
const loomitby = flip(lodashomitby);

exports.lomap = flip(lodashmap);
exports.lohas = flip(lodashhas);
exports.losumby = flip(lodashSumBy);
exports.logroupby = flip(lodashGroupBy);
exports.lofilter = flip(lodashFilter);
exports.louniqby = flip(lodashUniqBy);
exports.loreduce = curry((reducerFn, data) => lodashReduce(data, reducerFn));
exports.lokeyby = flip(lodashKeyBy);
exports.losortby = flip(lodashsortby);
exports.lochunk = lochunk;
exports.isNullString = (string) => string == "null";
exports.clean = clean;
exports.loget = loget;
exports.lohasin = lohasin;
exports.get_dates_range_array = get_dates_range_array;
exports.toSearchParams = toSearchParams;
exports.merge_by_key = merge_by_key;
exports.loorderby = loorderby;
exports.loomitby = loomitby;

exports.pipeLog = (value) => {
    console.log("pipeLog");
    console.log(util.inspect(value, false, null, true));
    return value;
};

exports.isEmail = validator.validate;
exports.isIPv4 = isIPv4;
exports.isIPv6 = isIPv6;
