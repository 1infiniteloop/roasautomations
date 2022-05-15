const { getDocs, query, where, collection, addDoc, collectionGroup, deleteDoc, doc, setDoc } = require("firebase/firestore");
const {
    from,
    map: rxmap,
    concatMap,
    lastValueFrom,
    reduce: rxreduce,
    zip,
    of: rxof,
    filter: rxfilter,
    throwError,
    defaultIfEmpty,
    tap,
} = require("rxjs");
const {
    identity,
    of,
    pipe,
    head,
    last,
    values,
    flatten,
    split,
    map,
    pick,
    mergeDeepRight,
    filter,
    gt,
    lt,
    gte,
    lte,
    equals,
    has,
    includes,
    keys,
    defaultTo,
    sum,
    reject,
    toLower,
} = require("ramda");
const { isEmpty, toNumber, size, isUndefined, isNull, flattenDeep } = require("lodash");
const { db } = require("./database");
const moment = require("moment");
const { formatInTimeZone } = require("date-fns-tz");
const { pipeLog, get_dates_range_array, lomap, loreduce, lokeyby, losortby, logroupby, lofilter, loorderby } = require("helpers");
var pluralize = require("pluralize");
const { get, all, mod, matching } = require("shades");
const { Account } = require("roasfacebook");

let numOrZero = (num) => {
    return num >= 0 && num !== Infinity ? num : 0;
};

const Rule = {
    utilities: {
        is_falsy: (value) => {
            if (isNull(value) || isUndefined(value) || value == "" || value == false) {
                return true;
            } else {
                return false;
            }
        },

        time_elapsed_since_last_check: (rule) => {
            let last_checked = Rule.utilities.is_falsy(rule.last_checked) ? moment().subtract(10, "years").unix() : rule.last_checked;
            let now = moment().unix();
            return moment(now, "X").diff(moment(last_checked, "X"), "minutes");
        },

        schedule_to_minutes: (amount, unit) => {
            if (unit == "minutes") {
                return amount;
            }

            if (unit == "hours") {
                return amount * 60;
            }

            if (unit == "days") {
                return amount * 1440;
            }
        },

        should_check_automation: (rule, time_elapsed) => {
            let {
                schedule: { unit, amount },
            } = rule;
            if (isUndefined(rule.last_checked)) return true;

            let schedule_in_minutes = Rule.utilities.schedule_to_minutes(amount, unit);

            if (time_elapsed > schedule_in_minutes) {
                return true;
            } else {
                return false;
            }
        },

        set_checked_at_timestamp: (rule) => {
            console.log("update_last_checked");
            let { id: rule_id } = rule;
            let payload = { last_checked: moment().unix() };
            setDoc(doc(db, "rules", rule_id), payload, { merge: true });
            return { ...rule, ...payload };
        },

        reset_active_checked_at_timestamp: (rule) => {
            console.log("reset_active_checked_at_timestamp");
            return from(setDoc(doc(db, "rules", rule.id), { last_checked: 0 }, { merge: true })).pipe(
                rxmap((value) => ({
                    rule_id: rule.id,
                    last_checked: 0,
                }))
            );
        },
    },

    logs: {
        save: ({ logs, rule_id }) => {
            return rxof(logs).pipe(
                rxmap(values),
                concatMap(identity),
                concatMap((log) => {
                    return from(addDoc(collection(db, "rules_logs"), { ...log, created_at: moment().unix() })).pipe(
                        rxmap(() => console.log(`saved rule ${rule_id} log ${log.asset_id}`)),
                        rxmap(() => log)
                    );
                }),
                rxmap(of)
            );
        },
    },
    scope: {
        get: (rule) => {
            let func_name = "Rule:scope:get";
            console.log(`${func_name}`);
            return rule.scope;
        },

        singular: (rule) => {
            let func_name = "Rule:scope:singular";
            console.log(`${func_name}`);
            return pluralize.singular(rule.scope);
        },
    },
    assets: {
        selected: {
            get: (rule) => {
                let func_name = "Rule:assets:selected:get";
                console.log(`${func_name}`);

                let { assets } = rule;
                let scope = Rule.scope.get(rule);
                return pipe(get(scope, matching({ selected: true })), defaultTo({}))(assets);
            },

            ids: (rule) => {
                let func_name = "Rule:assets:selected:ids";
                console.log(`${func_name}`);

                return pipe(Rule.assets.selected.get, keys, defaultTo([]))(rule);
            },
        },
    },
    action: {
        get: (rule) => {
            let {
                action: { value: action },
            } = rule;

            if (has("budget")) {
                return {
                    params: rule.budget,
                    method: action,
                };
            } else {
                return {
                    params: { method: action },
                };
            }
        },
    },
    actions: {
        prepare: {
            increase_budget: ({ params }, scope, asset) => {
                let today_pacific_time = formatInTimeZone(new Date(Date.now()), "America/Los_Angeles", "yyyy-MM-dd");
                let today = moment(today_pacific_time, "YYYY-MM-DD").format("YYYY-MM-DD");

                let payload = {
                    [pluralize.singular(scope)]: {
                        params: {
                            [`${pluralize.singular(scope)}_id`]: asset.asset_id,
                            time_range: { since: today, until: today },
                            action: "increase_budget",
                            type: params.type,
                            value: toNumber(params.value),
                        },
                    },
                };

                return payload;
            },

            decrease_budget: ({ params }, scope, asset) => {
                let today_pacific_time = formatInTimeZone(new Date(Date.now()), "America/Los_Angeles", "yyyy-MM-dd");
                let today = moment(today_pacific_time, "YYYY-MM-DD").format("YYYY-MM-DD");

                let payload = {
                    [pluralize.singular(scope)]: {
                        params: {
                            [`${pluralize.singular(scope)}_id`]: asset.asset_id,
                            time_range: { since: today, until: today },
                            action: "decrease_budget",
                            type: params.type,
                            value: toNumber(params.value),
                        },
                    },
                };

                return payload;
            },

            pause: (action, scope, asset) => {
                let func_name = `Rule:actions:prepare:pause`;
                console.log(func_name);

                let payload = {
                    [pluralize.singular(scope)]: {
                        params: {
                            [`${pluralize.singular(scope)}_id`]: asset.asset_id,
                            action: "stop",
                            status: "PAUSED",
                        },
                    },
                };

                return payload;
            },
        },
        execute: (account_id, access_token, action) => {
            return from(Account({ account_id, access_token }).post(action)).pipe(
                rxmap(head),
                defaultIfEmpty({}),
                rxmap(get("data")),
                defaultIfEmpty({})
            );
        },
    },
    predicates: {
        gt: gt,
        lt: lt,
        gte: gte,
        lte: lte,
        equals: equals,
    },
    validate: {
        self: (rule) => {
            let expressions = Rule.expressions.get(rule);

            let { user_id, assets, ...rest } = rule;
            let action = Rule.action.get(rule);
            let scope = Rule.scope.get(rule);
            let selected_assets_ids = Rule.assets.selected.ids(rule);

            return from(expressions).pipe(
                concatMap((expression) => Rule.expression.validate({ ...expression, scope, user_id, selected_assets_ids })),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(logroupby("asset_id")),
                rxmap(mod(all)((validations) => Rule.expressions.result.value(rule, validations))),
                rxmap((validations) => ({
                    passed: Rule.expressions.result.passed(validations),
                    failed: Rule.expressions.result.failed(validations),
                })),
                rxmap((validations) => ({ ...validations, ...rest, user_id, selected_ids: selected_assets_ids }))
            );
        },

        roas: (asset, predicate, value) => {
            let func_name = "Rule:validate:roas";
            console.log(`${func_name}`);

            if (asset == undefined || asset == null) return false;
            if (predicate == undefined || predicate == null) return false;
            if (value == undefined || value == null) return false;

            return Rule.predicates[`${predicate}`](Number(asset.roas), Number(value));
        },

        roasspend: (asset, predicate, value) => {
            let func_name = "Rule:validate:roasspend";
            console.log(`${func_name}`);

            if (asset == undefined || asset == null) return false;
            if (predicate == undefined || predicate == null) return false;
            if (value == undefined || value == null) return false;

            return Rule.predicates[`${predicate}`](Number(asset.fbspend), Number(value));
        },

        roassales: (asset, predicate, value) => {
            let func_name = "Rule:validate:roassales";
            console.log(`${func_name}`);

            if (asset == undefined || asset == null) return false;
            if (predicate == undefined || predicate == null) return false;
            if (value == undefined || value == null) return false;

            return Rule.predicates[`${predicate}`](Number(asset.roassales), Number(value));
        },
    },
    reports: {
        date_stats: (date, user_id, scope) => {
            let report_query = query(collection(db, "reports"), where("date", "==", date), where("user_id", "==", user_id));
            return from(getDocs(report_query)).pipe(
                rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())),
                defaultIfEmpty([])
                // rxmap(head),
                // rxfilter((report) => !isUndefined(report))
            );
        },

        merged_stats: (reports, scope) => {
            let func_name = "Rule:reports:merged_stats";
            console.log(`${func_name}`);

            let scope_singular = pluralize.singular(scope);
            let asset_id_prop_getter = `${scope_singular}_id`;
            let asset_name_prop_getter = `${scope_singular}_name`;

            let assets = pipe(
                get(all, scope),
                mod(all)(values),
                flatten,
                logroupby("asset_id"),
                mod(all)((asset) => ({
                    asset_id: pipe(get(all, "details", "asset_id"), head)(asset),
                    asset_name: pipe(get(all, "details", "asset_name"), head)(asset),
                    stats: pipe(get(all, "stats"))(asset),
                })),
                mod(
                    all,
                    "stats"
                )((stats) => ({
                    fbroas: pipe(get(all, "fbroas"), sum)(stats),
                    fbleads: pipe(get(all, "fbleads"), sum)(stats),
                    fbspend: pipe(get(all, "fbspend"), sum)(stats),
                    fbsales: pipe(get(all, "fbsales"), sum)(stats),
                    fbmade: pipe(get(all, "fbmade"), sum)(stats),
                    fbclicks: pipe(get(all, "fbclicks"), sum)(stats),
                })),
                mod(all)(({ stats, ...rest }) => ({ ...stats, ...rest }))
            )(reports);

            let customers = pipe(get(all, "customers"))(reports);

            let customer_ads = pipe(
                get(all, all, "ads"),
                mod(all)(values),
                flatten,
                mod(all, "email")(toLower),
                logroupby("email"),
                mod(all)(loorderby(["timestamp"], ["asc"])),
                mod(all)(last)
            )(customers);

            let customer_stats = pipe(
                get(all, all, "stats"),
                mod(all)(lomap((value, email) => ({ ...value, email }))),
                flatten,
                logroupby("email"),
                mod(all)((stats) => ({
                    email: pipe(get(all, "email"), head)(stats),
                    roasrevenue: pipe(get(all, "roasrevenue"), sum)(stats),
                    roassales: pipe(get(all, "roassales"), sum)(stats),
                }))
            )(customers);

            let customer_ad_stats = pipe(
                values,
                logroupby(asset_id_prop_getter),
                mod(all)((assets) => ({
                    roasrevenue: pipe(get(all, "roasrevenue"), sum)(assets),
                    roassales: pipe(get(all, "roassales"), sum)(assets),
                    asset_id: pipe(get(all, asset_id_prop_getter), head)(assets),
                    asset_name: pipe(get(all, asset_name_prop_getter), head)(assets),
                }))
            )(mergeDeepRight(customer_stats, customer_ads));

            let stats = pipe(
                identity,
                mod(all)((asset) => ({
                    ...asset,
                    roas: numOrZero(pipe(get("roasrevenue"), defaultTo(0))(asset) / pipe(get("fbspend"), defaultTo(0))(asset)),
                    roasspend: pipe(get("fbspend"), defaultTo(0))(asset),
                    roassales: pipe(get("roassales"), defaultTo(0))(asset),
                    asset_id: pipe(get("asset_id"))(asset) || pipe(get(asset_id_prop_getter))(asset),
                    asset_name: pipe(get("asset_name"))(asset) || pipe(get(asset_name_prop_getter))(asset),
                }))
            )(mergeDeepRight(assets, customer_ad_stats));

            return stats;
        },
    },
    expressions: {
        get: ({ conditions } = {}) => {
            let func_name = "Rule:expressions:get";
            console.log(`${func_name}`);

            if (!conditions) return;
            let expressions = pipe(get(all, "expressions"), values, flatten, mod(all)(values), flatten)(conditions);
            return expressions;
        },

        result: {
            value: (rule, validations) => {
                let func_name = "Rule:expressions:result:value";
                console.log(`${func_name}`);

                let { user_id, id: rule_id, action: rule_action, name, scope: rule_scope, budget = {} } = rule;

                let asset_id = pipe(get(all, "asset_id"), head)(validations);
                let asset_name = pipe(get(all, "asset_name"), head)(validations);
                let num_of_passed = pipe(
                    get(all, "status"),
                    reject((result) => result == false),
                    size
                )(validations);
                let num_of_failed = pipe(
                    get(all, "status"),
                    reject((result) => result == true),
                    size
                )(validations);
                let num_of_expressions = pipe(size)(validations);
                let validation = validations;
                let results = pipe(get(all, "status"))(validations);
                let status = pipe(includes(false))(results) ? "failed" : "passed";

                return {
                    asset_id,
                    asset_name,
                    num_of_passed,
                    num_of_failed,
                    num_of_expressions,
                    validation,
                    results,
                    status,
                    rule_id,
                    action: rule_action,
                    budget,
                    name,
                    user_id,
                    scope: rule_scope,
                };
            },

            passed: (validations) => {
                let func_name = "Rule:expressions:result:passed";
                console.log(`${func_name}`);
                return pipe(get(matching({ status: "passed" })))(validations);
            },

            failed: (validations) => {
                let func_name = "Rule:expressions:result:failed";
                console.log(`${func_name}`);
                return pipe(get(matching({ status: "failed" })))(validations);
            },
        },
    },
    expression: {
        dates_array: ({ timeframe } = {}) => {
            let func_name = "Rule:expression:time_range";
            console.log(`${func_name}`);

            if (!timeframe) return [];

            let time_range = pipe(get("args"))(timeframe);
            let { unit, value } = time_range;

            let start_date = moment().subtract(value, unit).format("YYYY-MM-DD");
            let end_date = moment().format("YYYY-MM-DD");

            let dates_range_array = Rules.utilities.get_dates_range_array(start_date, end_date);
            return dates_range_array;
        },

        validate: (expression) => {
            let func_name = "Rule:expression:validate";
            console.log(`${func_name}`);

            let dates = Rule.expression.dates_array(expression);

            return from(dates).pipe(
                concatMap((date) => Rule.reports.date_stats(date, expression.user_id, expression.scope)),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap((reports) => Rule.reports.merged_stats(reports, expression.scope)),
                rxmap(pipe(lofilter((asset, asset_id) => expression.selected_assets_ids.includes(asset_id)))),
                rxmap(values),
                concatMap(identity),
                rxmap((asset) => ({
                    ...asset,
                    asset_value: pipe(get(`${expression.metric.value}`))(asset),
                    status: Rule.validate[expression.metric.value](asset, expression.predicate.value, expression.value),
                    predicate: expression.predicate.value,
                    metric: expression.metric.value,
                    value: expression.value,
                    start_date: head(dates),
                    end_date: last(dates),
                    expression_id: expression.id,
                    condition_id: expression.condition_id,
                })),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr])
            );
        },
    },
};

const Rules = {
    utilities: {
        getDates: (startDate, endDate) => {
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
        },

        get_dates_range_array: (since, until) => {
            let start_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(since);

            let end_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(until);

            const dates = pipe(
                ([start_date, end_date]) => Rules.utilities.getDates(start_date, end_date),
                mod(all)((date) => moment(date, "YYYY-MM-DD").format("YYYY-MM-DD"))
            )([start_date, end_date]);

            return dates;
        },
    },

    get: {
        active: () => {
            let active_rules_query = query(collection(db, "rules"), where("status", "==", "active"));
            return from(getDocs(active_rules_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
        },
    },

    user: {
        get: {
            active: ({ user_id }) => {
                let active_rules_query = query(collection(db, "rules"), where("status", "==", "active"), where("user_id", "==", user_id));
                return from(getDocs(active_rules_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },
        },
    },
};

exports.Rules = Rules;
exports.Rule = Rule;
