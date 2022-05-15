const axios = require("axios");
const util = require("util");
const moment = require("moment");
const { formatInTimeZone } = require("date-fns-tz");
const {
    query,
    where,
    getDoc,
    getDocs,
    collection,
    collectionGroup,
    setDoc,
    doc,
    limit,
    orderBy,
    onSnapshot,
    arrayUnion,
    addDoc,
} = require("firebase/firestore");
const { db } = require("./database");
const { Account } = require("./facebook");
const { logroupby, lokeyby, pipeLog, louniqby, lomap, isEmail, isIPv4, isIPv6, lohas, loreduce } = require("helpers");
const {
    pick,
    map,
    pipe,
    values,
    mergeDeepRight,
    head,
    prop,
    identity,
    of,
    dissoc,
    defaultTo,
    path,
    ifElse,
    isNil,
    find,
    whereEq,
    add,
    mean,
} = require("ramda");
const { size, isUndefined, toNumber, isNaN, compact, isEmpty, flattenDeep, toLower, toUpper, concat } = require("lodash");
const {
    from,
    of: rxof,
    zip,
    map: rxmap,
    concatMap,
    catchError,
    throwError,
    lastValueFrom,
    defaultIfEmpty,
    reduce: rxreduce,
    tap,
    mapTo,
    filter: rxfilter,
    delay,
} = require("rxjs");
const { get, all, mod } = require("shades");

const Facebook = ({ user_id }) => {
    let utilities = {
        today_pacific_time: () => formatInTimeZone(new Date(Date.now()), "America/Los_Angeles", "yyyy-MM-dd"),
        today_pacific_date: () => moment(utilities.today_pacific_time(), "YYYY-MM-DD").format("YYYY-MM-DD"),

        metrics: (insight) => {
            let value = (data) => prop("value", data);
            let actions = (action_type) => pipe(prop("actions"), ifElse(isNil, identity, find(whereEq({ action_type }))))(insight);
            let actions_values = (action_type) => pipe(prop("action_values"), ifElse(isNil, identity, find(whereEq({ action_type }))))(insight);

            let fbspend = defaultTo(0, Number(prop("spend", insight)));
            let fbroas = defaultTo(0, Number(path(["purchase_roas", 0, "value"], insight)));
            let fbclicks = defaultTo(0, Number(value(actions("link_click"))));
            let fbsales = defaultTo(0, Number(value(actions("offsite_conversion.fb_pixel_purchase"))));
            let fbleads = defaultTo(0, Number(value(actions("offsite_conversion.fb_pixel_lead"))));
            let fbmade = defaultTo(0, Number(value(actions_values("offsite_conversion.fb_pixel_purchase"))));

            let result = {
                fbclicks,
                fbspend,
                fbmade,
                fbsales,
                fbroas,
                fbleads,
            };

            return result;
        },

        stats: (insights) =>
            pipe(
                map(utilities.metrics),
                loreduce((prev, curr) => ({
                    clicks: add(prev.clicks, curr.clicks),
                    spend: add(prev.spend, curr.spend),
                    made: add(prev.made, curr.made),
                    sales: add(prev.sales, curr.sales),
                    roas: mean([prev.roas, curr.roas]),
                    leads: add(prev.leads, curr.leads),
                }))
            )(insights),
    };

    let account = {
        credentials: () => {
            let func_name = "Facebook:account:credentials";
            console.log(`${func_name}`);

            return from(
                getDocs(query(collectionGroup(db, "integrations"), where("user_id", "==", user_id), where("account_name", "==", "facebook")))
            ).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                rxmap(pipe(head))
            );
        },

        active_ad_account: () => {
            let q = query(
                collection(db, "fb_ad_accounts"),
                where("user_ids", "array-contains", user_id),
                where("roas_account_status", "==", true),
                limit(1)
            );

            return from(getDocs(q)).pipe(
                rxmap((accounts) => {
                    let account = head(accounts.docs.map((account) => account.data()));
                    return account;
                })
            );
        },
    };

    let campaigns = {
        get: async ({ date, fb_ad_account_id } = {}) => {
            let func_name = "campaigns:get";
            console.log(func_name);

            let credentials = await lastValueFrom(account.credentials());

            if (!date) {
                date = utilities.today_pacific_date();
            }

            if (!credentials) return throwError(`error:${func_name}:no credentials`);
            if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

            let { access_token } = credentials;

            let fbaccount = Account({ fb_ad_account_id, access_token });

            // let fields = ["name", "daily_budget", "campaign_id", "account_id", "effective_status"];
            let fields = [];

            let campaigns_query = fbaccount.get({
                campaigns: {
                    path: ["campaigns", "*campaign_id"],
                    params: {
                        time_range: { since: date, until: date },
                        effective_status: ["ACTIVE"],
                        fields,
                    },
                },
            });

            let campaigns = from(campaigns_query).pipe(
                rxmap(prop("campaigns")),
                defaultIfEmpty({}),
                rxmap(
                    map((asset) => ({
                        ...asset,
                        type: "campaign",
                        is_cbo: asset.daily_budget ? true : false,
                        user_id,
                        fb_ad_account_id,
                        account_id: asset.account_id,
                        asset_id: asset.campaign_id,
                        asset_name: asset.name,
                        campaign_id: asset.campaign_id,
                        campaign_name: asset.name,
                    }))
                ),
                rxmap(values),
                concatMap(identity)
            );

            return campaigns;
        },

        insights: {
            get: ({ date } = {}) => {
                let func_name = "campaigns:insights";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                let campaigns = from(Facebook({ user_id }).campaigns.get({ date })).pipe(
                    concatMap(identity),
                    rxmap(map(pipe(pick(["campaign_id", "account_id", "name", "is_cbo"])))),
                    rxmap(values),
                    concatMap(identity),
                    concatMap((asset) =>
                        from(campaign.insights.get({ campaign_id: asset.campaign_id, date })).pipe(
                            concatMap(identity),
                            rxmap(mergeDeepRight(asset)),
                            rxmap(of)
                        )
                    ),
                    rxreduce((prev, curr) => [...prev, ...curr])
                );

                return campaigns;
            },

            update: ({ date }) => {
                let func_name = "campaigns:insights:update";
                console.log(func_name);

                return from(
                    getDocs(
                        query(
                            collectionGroup(db, "asset"),
                            where("date", "array-contains", date),
                            where("user_id", "==", user_id),
                            where("type", "==", "campaign")
                        )
                    )
                ).pipe(
                    rxmap((data) => data.docs.map((doc) => doc.data())),
                    concatMap(identity),
                    concatMap((asset) => {
                        let asset_id = pipe(get("details", "asset_id"))(asset);
                        let details = pipe(get("details"))(asset);
                        return from(Facebook({ user_id }).campaign.insights.get({ campaign_id: asset_id, date })).pipe(
                            concatMap(identity),
                            concatMap((campaign) => {
                                return Facebook({ user_id }).campaign.insights.set({
                                    data: { ...campaign, user_id, asset_id, type: "campaign", date, details },
                                    date,
                                });
                            })
                        );
                    })
                    // rxmap(pipeLog)
                );
            },
        },

        update: (params) => {
            let func_name = "campaigns:udpate";
            console.log(func_name);

            if (!params) return throwError(`error:${func_name}:no params`);

            let docs_query = getDocs(
                query(
                    collectionGroup(db, "asset"),
                    where("user_id", "==", user_id),
                    where("effective_status", "==", "ACTIVE"),
                    where("type", "==", "campaign")
                )
            );

            return from(docs_query).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                concatMap((assets) => {
                    return from(assets).pipe(
                        concatMap((asset) => {
                            let { campaign_id } = asset;
                            return Facebook({ user_id })
                                .campaign.update({ campaign_id, params })
                                .pipe(rxmap(() => ({ ...asset, ...params })));
                        }),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr])
                    );
                })
            );
        },
    };

    let campaign = {
        get: async ({ date, campaign_id } = {}) => {
            let func_name = "campaign:get";
            console.log(func_name);

            if (!date) {
                date = utilities.today_pacific_date();
            }

            let credentials = await lastValueFrom(account.credentials());
            let ad_account = await lastValueFrom(account.active_ad_account());

            if (!credentials) return throwError(`error:${func_name}:no credentials`);
            if (!ad_account) return throwError(`error:${func_name}:no ad_account`);

            let { access_token } = credentials;
            let { account_id } = ad_account;

            let fbaccount = Account({ account_id, access_token });

            let campaign = fbaccount.get({
                campaign: {
                    path: ["*campaign_id"],
                    params: { time_range: { since: date, until: date }, campaign_id },
                },
            });

            return campaign;
        },

        get_from_db: async ({ campaign_id } = {}) => {
            let func_name = "campaign:get_from_db";
            console.log(func_name);

            if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);

            return from(getDocs(query(collectionGroup(db, "asset"), where("asset_id", "==", campaign_id)))).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                rxmap(head),
                rxfilter((data) => !isUndefined(data)),
                defaultIfEmpty({})
            );
        },

        set: async (data) => {
            let func_name = "campaign:set";
            console.log(func_name);

            if (!data) return throwError(`error:${func_name}:no data`);

            let { campaign_id, campaign_name, date } = data;

            if (campaign_name) {
                let payload = {
                    ...data,
                    date: arrayUnion(date),
                    user_ids: arrayUnion(user_id),
                };

                payload = pipe(dissoc("user_id"))(payload);

                // console.log("campaignpayloaddata");
                // console.log(payload);

                return setDoc(doc(db, "campaigns", campaign_id, "asset", campaign_id), payload, { merge: true }).then(() => {
                    console.log(`${func_name}:saved:${campaign_id}`);
                    return data;
                });
            } else {
                return data;
            }
        },

        update: ({ campaign_id, params }) => {
            let func_name = "campaign:udpate";
            console.log(func_name);

            if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);
            if (!params) return throwError(`error:${func_name}:no params`);

            return from(setDoc(doc(db, "campaigns", campaign_id, "asset", campaign_id), params, { merge: true }));
        },

        adsets: {
            get: async ({ campaign_id, date, fb_ad_account_id, fields = [] } = {}) => {
                let func_name = "campaign:adsets:get";
                console.log(func_name);

                let credentials = await lastValueFrom(account.credentials());

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!campaign_id) return throwError(`error:${func_name}:no campaign id`);
                if (!credentials) return throwError(`error:${func_name}:no credentials`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let { access_token } = credentials;

                let fbaccount = Account({ fb_ad_account_id, access_token });

                let adsets_query = fbaccount.get({
                    adsets: {
                        path: ["adsets", "*adset_id"],
                        params: { time_range: { since: date, until: date }, effective_status: ["ACTIVE"], campaign_id, fields: [] },
                    },
                });

                let adsets = from(adsets_query).pipe(
                    rxmap(prop("adsets")),
                    defaultIfEmpty({}),
                    rxmap(
                        mod(all)((asset) =>
                            asset.daily_budget
                                ? { ...asset, is_abo: true, user_id, type: "adset" }
                                : { ...asset, is_abo: false, user_id, type: "adset" }
                        )
                    )
                );

                return adsets;
            },

            stream: async ({ campaign_id } = {}) => {
                return campaign.adsets.get(campaign_id);
            },
        },

        insights: {
            get: async ({ campaign_id, date, fb_ad_account_id, fields = [] } = {}) => {
                let func_name = "campaign:insights:get";
                console.log(func_name);

                let credentials = await lastValueFrom(account.credentials());

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);
                if (!credentials) return throwError(`error:${func_name}:no credentials`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let { access_token } = credentials;

                let fbaccount = Account({ fb_ad_account_id, access_token });

                let insights_query = fbaccount.get({
                    campaign_insights: {
                        path: ["*campaign_id"],
                        params: { time_range: { since: date, until: date }, campaign_id, fields },
                    },
                });

                return from(insights_query).pipe(
                    concatMap(values),
                    rxmap((value) => {
                        let stats = pipe(
                            utilities.stats,
                            defaultTo({ fbclicks: 0, fbspend: 0, fbmade: 0, fbsales: 0, fbroas: 0, fbleads: 0 })
                        )(value.insights);

                        return { user_id, campaign_id, asset_id: campaign_id, date, fb_ad_account_id, type: "campaign", ...stats };
                    })
                );
            },

            set: ({ data, date } = {}) => {
                let func_name = "campaign:insights:set";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!data) return throwError(`error:${func_name}:no data`);

                let now = new Date().getTime();

                let {
                    user_id,
                    fbclicks = 0,
                    fbsales = 0,
                    fbleads = 0,
                    fbspend = 0,
                    fbmade = 0,
                    fbroas = 0,
                    fb_ad_account_id,
                    asset_id,
                    asset_name,
                    campaign_id,
                    campaign_name,
                    is_cbo,
                } = data;

                let insight = {
                    fbclicks,
                    fbsales,
                    fbleads,
                    fbspend,
                    fbmade,
                    fbroas,
                };

                let details = {
                    fb_ad_account_id,
                    asset_id,
                    asset_name,
                    campaign_id,
                    campaign_name,
                    is_cbo,
                };

                let payload = {
                    insight,
                    details,
                    date,
                    created_at: now,
                    user_ids: arrayUnion(user_id),
                    campaign_id,
                    fb_ad_account_id,
                    type: "campaign",
                };

                return from(setDoc(doc(db, "campaign_insights", campaign_id, "insights", `${now}`), payload)).pipe(
                    concatMap(() =>
                        from(setDoc(doc(db, "campaign_insights", campaign_id, "insight", date), payload, { merge: false })).pipe(
                            rxmap(() => console.log(`saved campaign ${campaign_id} insight`)),
                            rxmap(() => payload)
                        )
                    )
                );
            },
        },
    };

    let adsets = {
        get: async ({ date, fb_ad_account_id } = {}) => {
            let func_name = "campaigns:get";
            console.log(func_name);

            let credentials = await lastValueFrom(account.credentials());

            if (!date) {
                date = utilities.today_pacific_date();
            }

            if (!credentials) return throwError(`error:${func_name}:no credentials`);
            if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

            return from(campaigns.get({ date, fb_ad_account_id })).pipe(
                concatMap(identity),
                concatMap(({ campaign_id, name: campaign_name }) => {
                    // let fields = ["name", "daily_budget", "adset_id", "account_id", "campaign_id", "effective_status"];
                    let fields = undefined;

                    return from(campaign.adsets.get({ campaign_id, date, fb_ad_account_id, fields })).pipe(
                        concatMap(identity),
                        rxmap(values),
                        rxmap(
                            map((asset) => ({
                                ...asset,
                                type: "adset",
                                is_abo: asset.daily_budget ? true : false,
                                user_id,
                                fb_ad_account_id,
                                account_id: asset.account_id,
                                asset_id: asset.adset_id,
                                asset_name: asset.name,
                                campaign_id,
                                campaign_name,
                                adset_id: asset.adset_id,
                                adset_name: asset.name,
                            }))
                        )
                    );
                }),
                concatMap(identity)
            );
        },

        update: (params) => {
            let func_name = "adsets:udpate";
            console.log(func_name);

            if (!params) return throwError(`error:${func_name}:no params`);

            let docs_query = getDocs(
                query(
                    collectionGroup(db, "asset"),
                    where("user_id", "==", user_id),
                    where("effective_status", "==", "ACTIVE"),
                    where("type", "==", "adset")
                )
            );
            return from(docs_query).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                concatMap((assets) => {
                    return from(assets).pipe(
                        concatMap((asset) => {
                            let { adset_id } = asset;
                            return Facebook({ user_id })
                                .adset.update({ adset_id, params })
                                .pipe(rxmap(() => ({ ...asset, ...params })));
                        }),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr])
                    );
                })
            );
        },

        insights: {
            get: ({ date, adsets } = {}) => {
                let func_name = "adsets:insights:get";
                console.log(func_name);
                // console.log(adsets);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!adsets) return throwError(`error:${func_name}:no adsets`);

                let result = rxof(values(adsets)).pipe(
                    concatMap(identity),
                    concatMap((result) =>
                        from(adset.insights.get({ adset_id: result.adset_id, date })).pipe(
                            concatMap(identity),
                            rxmap(mergeDeepRight(result)),
                            rxmap(of)
                        )
                    ),
                    rxreduce((prev, curr) => [...prev, ...curr])
                );

                return result;
            },

            all: ({ date } = {}) => {
                let func_name = "adsets:insights:all";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                return from(Facebook({ user_id }).campaigns.get({ date })).pipe(
                    concatMap(identity),
                    rxmap(values),
                    concatMap(identity),
                    concatMap((campaign_result) =>
                        from(campaign.adsets.get({ campaign_id: campaign_result.campaign_id })).pipe(
                            concatMap(identity),
                            rxmap(values),
                            concatMap(identity),
                            concatMap((adset_result) =>
                                from(adset.insights.get({ adset_id: adset_result.adset_id })).pipe(
                                    concatMap(identity),
                                    rxmap(mergeDeepRight(adset_result)),
                                    rxmap(of)
                                )
                            ),
                            rxreduce((prev, curr) => [...prev, ...curr])
                        )
                    ),
                    rxreduce((prev, curr) => [...prev, ...curr])
                );
            },

            update: ({ date }) => {
                return from(
                    getDocs(
                        query(
                            collectionGroup(db, "asset"),
                            where("date", "array-contains", date),
                            where("user_id", "==", user_id),
                            where("type", "==", "adset")
                        )
                    )
                ).pipe(
                    rxmap((data) => data.docs.map((doc) => doc.data())),
                    concatMap(identity),
                    concatMap((asset) => {
                        let asset_id = pipe(get("details", "asset_id"))(asset);
                        let details = pipe(get("details"))(asset);
                        return from(Facebook({ user_id }).adset.insights.get({ adset_id: asset_id, date })).pipe(
                            concatMap(identity),
                            concatMap((insight) =>
                                Facebook({ user_id }).adset.insights.set({
                                    data: { ...insight, user_id, asset_id, type: "adset", date, details },
                                    date,
                                })
                            )
                        );
                    })
                    // rxmap(pipeLog)
                );
            },
        },
    };

    let adset = {
        update: ({ adset_id, params }) => {
            let func_name = "adset:udpate";
            console.log(func_name);

            if (!adset_id) return throwError(`error:${func_name}:no adset_id`);
            if (!params) return throwError(`error:${func_name}:no params`);

            return from(setDoc(doc(db, "adsets", adset_id, "asset", adset_id), params, { merge: true }));
        },

        set: async (data) => {
            let func_name = "adset:set";
            console.log(func_name);

            if (!data) return throwError(`error:${func_name}:no data`);

            let { adset_id, adset_name, date } = data;

            if (adset_name) {
                let payload = {
                    ...data,
                    date: arrayUnion(date),
                    user_ids: arrayUnion(user_id),
                };

                payload = pipe(dissoc("user_id"))(payload);

                return setDoc(doc(db, "adsets", adset_id, "asset", adset_id), payload, { merge: true }).then(() => {
                    console.log(`${func_name}:saved`);
                    return data;
                });
            } else {
                return data;
            }
        },

        get_from_db: async ({ adset_id } = {}) => {
            let func_name = "adset:get_from_db";
            console.log(func_name);

            if (!adset_id) return throwError(`error:${func_name}:no adset_id`);

            return from(getDocs(query(collectionGroup(db, "asset"), where("asset_id", "==", adset_id)))).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                rxmap(head),
                rxfilter((data) => !isUndefined(data)),
                defaultIfEmpty({})
            );
        },

        get: async ({ date, adset_id } = {}) => {
            let func_name = "adset:get";
            console.log(func_name);

            if (!date) {
                date = utilities.today_pacific_date();
            }

            let credentials = await lastValueFrom(account.credentials());
            let ad_account = await lastValueFrom(account.active_ad_account());

            if (!credentials) return throwError(`error:${func_name}:no credentials`);
            if (!ad_account) return throwError(`error:${func_name}:no ad_account`);

            let { access_token } = credentials;
            let { account_id } = ad_account;

            let fbaccount = Account({ account_id, access_token });

            let adset = fbaccount.get({
                adset: {
                    path: ["*adset_id"],
                    params: { time_range: { since: date, until: date }, adset_id },
                },
            });

            return adset;
        },

        ads: {
            get: async ({ adset_id, date, fb_ad_account_id, fields = [] } = {}) => {
                let func_name = "adset:ads:get";
                console.log(func_name);

                let credentials = await lastValueFrom(account.credentials());

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!adset_id) return throwError(`error:${func_name}:no adset id`);
                if (!credentials) return throwError(`error:${func_name}:no credentials`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let { access_token } = credentials;

                let fbaccount = Account({ fb_ad_account_id, access_token });

                let ads_query = fbaccount.get({
                    ads: {
                        path: ["*ad_id"],
                        params: { time_range: { since: date, until: date }, effective_status: ["ACTIVE"], adset_id, fields: undefined },
                    },
                });

                let ads = from(ads_query).pipe(defaultIfEmpty({}));

                return ads;
            },
        },

        insights: {
            get: async ({ adset_id, date, fb_ad_account_id, fields = [] } = {}) => {
                let func_name = "adset:insights:get";
                console.log(func_name);

                let credentials = await lastValueFrom(account.credentials());

                if (!date) {
                    date = utilities.today_pacific_date();
                }
                if (!adset_id) return throwError(`error:${func_name}:no adset id`);
                if (!credentials) return throwError(`error:${func_name}:no credentials`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let { access_token } = credentials;

                let fbaccount = Account({ fb_ad_account_id, access_token });

                let insights_query = fbaccount.get({
                    adset_insights: {
                        path: ["*adset_id"],
                        params: { time_range: { since: date, until: date }, adset_id, fields },
                    },
                });

                return from(insights_query).pipe(
                    concatMap(values),
                    rxmap((value) => {
                        let stats = pipe(
                            utilities.stats,
                            defaultTo({ fbclicks: 0, fbspend: 0, fbmade: 0, fbsales: 0, fbroas: 0, fbleads: 0 })
                        )(value.insights);

                        return { user_id, adset_id, asset_id: adset_id, date, fb_ad_account_id, type: "adset", ...stats };
                    })
                );
            },

            set: ({ data, date } = {}) => {
                let func_name = "adset:insights:set";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!data) return throwError(`error:${func_name}:no data`);

                let now = new Date().getTime();

                let {
                    user_id,
                    fbclicks = 0,
                    fbsales = 0,
                    fbleads = 0,
                    fbspend = 0,
                    fbmade = 0,
                    fbroas = 0,
                    fb_ad_account_id,
                    asset_id,
                    asset_name,
                    campaign_name,
                    campaign_id,
                    adset_id,
                    adset_name,
                    is_abo,
                } = data;

                let insight = {
                    fbclicks,
                    fbsales,
                    fbleads,
                    fbspend,
                    fbmade,
                    fbroas,
                };

                let details = {
                    fb_ad_account_id,
                    asset_id,
                    asset_name,
                    adset_id,
                    adset_name,
                    campaign_name,
                    campaign_id,
                    is_abo,
                };

                let payload = {
                    insight,
                    details,
                    date,
                    created_at: now,
                    user_ids: arrayUnion(user_id),
                    adset_id,
                    fb_ad_account_id,
                    type: "adset",
                };

                return from(setDoc(doc(db, "adset_insights", adset_id, "insights", `${now}`), payload)).pipe(
                    concatMap(() =>
                        from(setDoc(doc(db, "adset_insights", adset_id, "insight", date), payload, { merge: false })).pipe(
                            rxmap(() => console.log(`saved adset ${adset_id} insight`)),
                            rxmap(() => payload)
                        )
                    )
                );
            },
        },
    };

    let ads = {
        get: async ({ date, fb_ad_account_id } = {}) => {
            let func_name = "ads:get";
            console.log(func_name);

            let credentials = await lastValueFrom(account.credentials());

            if (!date) {
                date = utilities.today_pacific_date();
            }

            if (!credentials) return throwError(`error:${func_name}:no credentials`);
            if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

            return from(adsets.get({ date, fb_ad_account_id })).pipe(
                concatMap(identity),
                // rxmap(pipeLog),
                concatMap(({ adset_id, campaign_id, campaign_name, adset_name }) => {
                    // let fields = ["name", "ad_id", "account_id", "effective_status"];
                    let fields = undefined;

                    return from(adset.ads.get({ date, adset_id, fb_ad_account_id, fields })).pipe(
                        concatMap(identity),
                        rxmap(values),
                        rxmap(
                            map((asset) => ({
                                ...asset,
                                user_id,
                                type: "ad",
                                fb_ad_account_id,
                                account_id: asset.account_id,
                                asset_id: asset.ad_id,
                                asset_name: asset.name,
                                campaign_id,
                                campaign_name,
                                adset_id,
                                adset_name,
                                ad_id: asset.ad_id,
                                ad_name: asset.name,
                            }))
                        )
                    );
                }),
                concatMap(identity)
            );
        },

        update: (params) => {
            let func_name = "ads:udpate";
            console.log(func_name);

            if (!params) return throwError(`error:${func_name}:no params`);

            let docs_query = getDocs(
                query(
                    collectionGroup(db, "ads"),
                    where("user_id", "==", user_id),
                    where("effective_status", "==", "ACTIVE"),
                    where("type", "==", "ad")
                )
            );
            return from(docs_query).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                concatMap((assets) => {
                    return from(assets).pipe(
                        concatMap((asset) => {
                            let { adset_id } = asset;
                            return Facebook({ user_id })
                                .ad.update({ ad_id, params })
                                .pipe(rxmap(() => ({ ...asset, ...params })));
                        }),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr])
                    );
                })
            );
        },

        insights: {
            update: ({ date }) => {
                return from(
                    getDocs(
                        query(
                            collectionGroup(db, "asset"),
                            where("date", "array-contains", date),
                            where("user_id", "==", user_id),
                            where("type", "==", "ad")
                        )
                    )
                ).pipe(
                    rxmap((data) => data.docs.map((doc) => doc.data())),
                    concatMap(identity),
                    concatMap((asset) => {
                        let asset_id = pipe(get("details", "asset_id"))(asset);
                        let details = pipe(get("details"))(asset);
                        return from(Facebook({ user_id }).ad.insights.get({ ad_id: asset_id, date })).pipe(
                            concatMap(identity),
                            concatMap((insight) =>
                                Facebook({ user_id }).ad.insights.set({
                                    data: { ...insight, user_id, asset_id, type: "ad", date, details },
                                    date,
                                })
                            )
                        );
                    })
                    // rxmap(pipeLog)
                );
            },
        },
    };

    let ad = {
        update: ({ ad_id, params }) => {
            let func_name = "ad:udpate";
            console.log(func_name);

            if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
            if (!params) return throwError(`error:${func_name}:no params`);

            return from(setDoc(doc(db, "ads", ad_id, "asset", ad_id), params, { merge: true }));
        },

        get: async ({ date, ad_id, fb_ad_account_id, fields = [] } = {}) => {
            let func_name = "ad:get";
            console.log(func_name);
            console.log(user_id);

            let credentials = await lastValueFrom(account.credentials());

            // console.log(`log:credentials:${func_name}`);
            // console.log(credentials);

            if (!date) {
                date = utilities.today_pacific_date();
            }

            if (!credentials) return throwError(`error:${func_name}:no credentials`);
            if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);
            if (!ad_id) return throwError(`error:${func_name}:no ad_id`);

            let { access_token } = credentials;

            let fbaccount = Account({ fb_ad_account_id, access_token });

            let ad = fbaccount.get({
                ad: {
                    path: ["*ad_id"],
                    params: { time_range: { since: date, until: date }, ad_id, fields: undefined },
                },
            });

            return ad;
        },

        get_from_db: async ({ ad_id } = {}) => {
            let func_name = "ad:get_from_db";
            console.log(func_name);

            if (!ad_id) return throwError(`error:${func_name}:no ad_id`);

            return from(getDocs(query(collectionGroup(db, "asset"), where("asset_id", "==", ad_id)))).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                // rxmap(pipeLog),
                rxmap(head),
                rxfilter((data) => !isUndefined(data)),
                defaultIfEmpty({})
            );
        },

        set: async (data) => {
            let func_name = "ad:set";
            console.log(func_name);

            if (!data) return throwError(`error:${func_name}:no data`);

            let { ad_id, ad_name, date } = data;

            if (!date) {
                date = utilities.today_pacific_date();
            }

            if (ad_name) {
                let payload = {
                    ...data,
                    date: arrayUnion(date),
                    user_ids: arrayUnion(user_id),
                };

                payload = pipe(dissoc("user_id"))(payload);

                console.log("setaddata");
                console.log(date);
                console.log(payload);

                return setDoc(doc(db, "ads", ad_id, "asset", ad_id), payload, { merge: true }).then(() => {
                    console.log(`${func_name}:saved:${ad_id}`);
                    return data;
                });
            } else {
                return data;
            }
        },

        insights: {
            get: async ({ ad_id, date, fb_ad_account_id, fields = [] } = {}) => {
                let func_name = "ad:insights:get";
                console.log(func_name);

                let credentials = await lastValueFrom(account.credentials());

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!credentials) return throwError(`error:${func_name}:no credentials`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let { access_token } = credentials;

                let fbaccount = Account({ fb_ad_account_id, access_token });

                let insights_query = fbaccount.get({
                    ad_insights: {
                        path: ["*ad_id"],
                        params: { time_range: { since: date, until: date }, ad_id, fields },
                    },
                });

                return from(insights_query).pipe(
                    concatMap(values),
                    rxmap((value) => {
                        let stats = pipe(
                            utilities.stats,
                            defaultTo({ fbclicks: 0, fbspend: 0, fbmade: 0, fbsales: 0, fbroas: 0, fbleads: 0 })
                        )(value.insights);

                        return { user_id, ad_id, asset_id: ad_id, date, fb_ad_account_id, type: "ad", ...stats };
                    }),
                    catchError((error) => {
                        console.log("error:facebook");
                        console.log(error);
                        return rxof({});
                    })
                );
            },

            set: ({ data, date } = {}) => {
                let func_name = "ad:insights:set";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                if (!data) return throwError(`error:${func_name}:no data`);

                let now = new Date().getTime();
                let {
                    user_id,
                    fbclicks = 0,
                    fbsales = 0,
                    fbleads = 0,
                    fbspend = 0,
                    fbmade = 0,
                    fbroas = 0,
                    fb_ad_account_id,
                    asset_id,
                    asset_name,
                    ad_id,
                    ad_name,
                    campaign_id,
                    campaign_name,
                    adset_id,
                    adset_name,
                } = data;

                let insight = {
                    fbclicks,
                    fbsales,
                    fbleads,
                    fbspend,
                    fbmade,
                    fbroas,
                };

                let details = {
                    fb_ad_account_id,
                    asset_id,
                    asset_name,
                    ad_id,
                    ad_name,
                    campaign_id,
                    campaign_name,
                    adset_id,
                    adset_name,
                };

                let payload = {
                    insight,
                    details,
                    date,
                    created_at: now,
                    user_ids: arrayUnion(user_id),
                    ad_id,
                    fb_ad_account_id,
                    type: "ad",
                };

                return from(setDoc(doc(db, "ad_insights", ad_id, "insights", `${now}`), payload)).pipe(
                    concatMap(() =>
                        from(setDoc(doc(db, "ad_insights", ad_id, "insight", date), payload, { merge: false })).pipe(
                            rxmap(() => console.log(`saved ad ${ad_id} insight`)),
                            // rxmap(() => pipeLog(payload)),
                            rxmap(() => payload)
                        )
                    ),
                    catchError((error) => {
                        console.log("error:facebook");
                        console.log(error);
                        return rxof({});
                    })
                );
            },
        },
    };

    let stats = {
        campaigns: {
            get: ({ date } = {}) => {
                let func_name = "Roas:stats:campaigns:get";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                return Facebook({ user_id }).campaigns.insights({ date }).pipe(concatMap(identity), rxmap(of));
            },

            set: ({ data, created_at, date } = {}) => {
                let func_name = "Facebook:stats:campaigns:set";
                console.log(func_name);

                if (!date) return throwError(`error:${func_name}:no date`);
                if (!data) return throwError(`error:${func_name}:no data`);
                if (!created_at) return throwError(`error:${func_name}:no created_at`);

                // console.log(`log:${func_name}:created_at`);
                // console.log(created_at);

                // console.log(`log:${func_name}:data`);
                // console.log(data);

                let { campaign_id } = data;
                let doc_id = `${created_at}campaignsfacebook`;

                let stats = pipe(prop("stats"))(data);

                let payload = {
                    data: { [campaign_id]: data },
                    stats: { [campaign_id]: stats },
                    source_type: "ad",
                    source: "facebook",
                    asset_type: "campaigns",
                    created_at,
                    user_id,
                    date,
                };

                return from(setDoc(doc(db, "stats", doc_id), payload, { merge: true })).pipe(
                    rxmap(() => `success:${func_name}:saved campaign ${campaign_id} at ${created_at} for user ${user_id}`)
                );
            },
        },

        adsets: {
            get: ({ date } = {}) => {
                let func_name = "Roas:stats:adsets";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                let campaigns_res = from(Facebook({ user_id }).campaigns.get({ date })).pipe(concatMap(identity), rxmap(values));

                let adsets_res = campaigns_res.pipe(
                    concatMap(identity),
                    concatMap((result) =>
                        from(Facebook({ user_id }).campaign.adsets.get({ campaign_id: result.campaign_id, date })).pipe(concatMap(identity))
                    ),
                    concatMap((adsets) => {
                        console.log(`${func_name}`);

                        if (adsets) {
                            return Facebook({ user_id }).adsets.insights.get({ date, adsets });
                        } else {
                            return rxof([]);
                        }
                    })
                );

                return adsets_res;
            },

            set: ({ data, created_at, date } = {}) => {
                let func_name = "Facebook:stats:adsets:set";
                console.log(func_name);

                if (!date) return throwError(`error:${func_name}:no date`);
                if (!data) return throwError(`error:${func_name}:no data`);
                if (!created_at) return throwError(`error:${func_name}:no created_at`);

                // console.log(`log:${func_name}:created_at`);
                // console.log(created_at);

                // console.log(`log:${func_name}:data`);
                // console.log(data);

                let { adset_id } = data;
                let doc_id = `${created_at}adsetsfacebook`;

                let stats = pipe(prop("stats"))(data);

                let asset_details = pipe(prop("insights"), head, pick(["adset_id", "adset_name", "campaign_id", "campaign_name"]), (asset) => ({
                    asset_id: asset.adset_id,
                    asset_name: asset.adset_name,
                    campaign_id: asset.campaign_id,
                    campaign_name: asset.campaign_name,
                    adset_id: asset.adset_id,
                    adset_name: asset.adset_name,
                }));

                let payload = {
                    data: { [adset_id]: data },
                    stats: { [adset_id]: stats },
                    details: { [adset_id]: asset_details(data) },
                    source_type: "ad",
                    source: "facebook",
                    asset_type: "adsets",
                    created_at,
                    user_id,
                    date,
                };

                return from(setDoc(doc(db, "stats", doc_id), payload, { merge: true })).pipe(
                    rxmap(() => `success:${func_name}:saved adset ${adset_id} at ${created_at} for user ${user_id}`)
                );
            },
        },

        ads: {
            get: ({ date } = {}) => {
                let func_name = "Roas:stats:ads:get";
                console.log(func_name);

                if (!date) {
                    date = utilities.today_pacific_date();
                }

                let campaigns_res = from(Facebook({ user_id }).campaigns.get({ date })).pipe(concatMap(identity), rxmap(values));

                let adsets_res = campaigns_res.pipe(
                    concatMap(identity),
                    concatMap((campaign) =>
                        from(Facebook({ user_id }).campaign.adsets.get({ campaign_id: campaign.campaign_id, date })).pipe(
                            concatMap(identity),
                            rxmap(values)
                        )
                    )
                );

                let ads_res = adsets_res.pipe(
                    concatMap(identity),
                    concatMap((adset) => from(Facebook({ user_id }).adset.ads.get({ adset_id: adset.adset_id })).pipe(concatMap(identity))),
                    rxmap(values),
                    concatMap(identity),
                    concatMap((ad) => from(Facebook({ user_id }).ad.insights.get({ date, ad_id: ad.ad_id })).pipe(concatMap(identity), rxmap(of)))
                );

                return ads_res;
            },

            set: ({ data, created_at, date } = {}) => {
                let func_name = "Facebook:stats:ads:set";
                console.log(func_name);

                if (!date) return throwError(`error:${func_name}:no date`);
                if (!data) return throwError(`error:${func_name}:no data`);
                if (!created_at) return throwError(`error:${func_name}:no created_at`);

                let { ad_id } = data;
                let doc_id = `${created_at}adsfacebook`;

                let stats = pipe(prop("stats"))(data);

                let payload = {
                    data: { [ad_id]: data },
                    stats: { [ad_id]: stats },
                    source_type: "ad",
                    source: "facebook",
                    asset_type: "ads",
                    created_at,
                    user_id,
                    date,
                };

                return from(setDoc(doc(db, "stats", doc_id), payload, { merge: false })).pipe(
                    rxmap(() => `success:${func_name}:saved ad ${ad_id} at ${created_at} for user ${user_id}`)
                );
            },
        },
    };

    let services = {
        campaigns: {
            update_insights_listener: () => {
                var insights_init_state = true;
                onSnapshot(query(collection(db, "services", "insights", "listener")), (querySnapshot) => {
                    console.log("campaignlistner");
                    if (insights_init_state) {
                        insights_init_state = false;
                    } else {
                        querySnapshot.docChanges().forEach(async (change) => {
                            console.log("changetype");
                            console.log(change.type);

                            if (change.type === "added") {
                                let data = change.doc.data();
                                let { user_id, asset_id, type, date, details } = data;

                                if (type == "campaign") {
                                    console.log("modifiedactivead");
                                    from(Facebook({ user_id }).campaign.insights.get({ campaign_id: asset_id, date }))
                                        .pipe(
                                            concatMap(identity),
                                            concatMap((campaign) =>
                                                Facebook({ user_id }).campaign.insights.set({
                                                    data: { ...campaign, user_id, asset_id, type, date, details },
                                                    date,
                                                })
                                            )
                                        )
                                        .subscribe();
                                }

                                console.log("ALLDONE");
                                return "done";
                            }
                        });
                    }
                });
            },
        },

        adsets: {
            update_insights_listener: () => {
                var insights_init_state = true;
                onSnapshot(query(collection(db, "services", "insights", "listener")), (querySnapshot) => {
                    console.log("adsetlistner");
                    if (insights_init_state) {
                        insights_init_state = false;
                    } else {
                        querySnapshot.docChanges().forEach(async (change) => {
                            console.log("changetype");
                            console.log(change.type);

                            if (change.type === "added") {
                                let data = change.doc.data();
                                let { user_id, asset_id, type, date, details } = data;

                                if (type == "adset") {
                                    console.log("modifiedactiveadset");
                                    from(Facebook({ user_id }).adset.insights.get({ adset_id: asset_id, date }))
                                        .pipe(
                                            concatMap(identity),
                                            concatMap((adset) =>
                                                Facebook({ user_id }).adset.insights.set({
                                                    data: { ...adset, user_id, asset_id, type, date, details },
                                                    date,
                                                })
                                            )
                                        )
                                        .subscribe();
                                }

                                console.log("ALLDONE");
                                return "done";
                            }
                        });
                    }
                });
            },
        },

        ads: {
            update_insights_listener: () => {
                var insights_init_state = true;
                onSnapshot(query(collection(db, "services", "insights", "listener")), (querySnapshot) => {
                    console.log("listners");
                    if (insights_init_state) {
                        insights_init_state = false;
                    } else {
                        querySnapshot.docChanges().forEach(async (change) => {
                            console.log("changetype");
                            console.log(change.type);

                            if (change.type === "added") {
                                let data = change.doc.data();
                                let { user_id, asset_id, type, date, details } = data;

                                if (type == "ad") {
                                    console.log("modifiedactivead");

                                    from(Facebook({ user_id }).ad.insights.get({ ad_id: asset_id, date }))
                                        .pipe(
                                            // rxmap(pipeLog),
                                            concatMap(identity),
                                            concatMap((ad) =>
                                                Facebook({ user_id }).ad.insights.set({
                                                    data: { ...ad, user_id, asset_id, type, date, details },
                                                    date,
                                                })
                                            )
                                        )
                                        .subscribe();
                                }

                                console.log("ALLDONE");
                                return "done";
                            }
                        });
                    }
                });
            },
        },

        update_facebook_assets: async ({ date, type, project_account_id, fb_ad_account_id }) => {
            let func_name = "services:update_facebook_assets";
            console.log(func_name);

            if (!date) {
                date = utilities.today_pacific_date();
            }

            if (!project_account_id) return throwError(`error:${func_name}:no project_account_id`);

            let asset_collection_map = {
                campaigns: "campaigns",
                adsets: "adsets",
                ads: "ads",
            };

            let asset_instance_map = {
                campaigns: "campaign",
                adsets: "adset",
                ads: "ad",
            };

            let asset_collection_path = asset_collection_map[type];
            let asset_instance_path = asset_instance_map[type];

            return from(Facebook({ user_id })[asset_collection_path].get({ date, fb_ad_account_id })).pipe(
                concatMap(identity),
                concatMap((asset) => from(Facebook({ user_id })[asset_instance_path].set({ ...asset, date, fb_ad_account_id }))),
                catchError((error) => throwError(error)),
                defaultIfEmpty({})
            );
        },
    };

    return {
        utilities,
        account,
        campaigns,
        campaign,
        adsets,
        adset,
        ads,
        ad,
        stats,
        services,
    };
};

exports.Facebook = Facebook;
exports.Account = Account;
