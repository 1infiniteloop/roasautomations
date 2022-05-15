const axios = require("axios");
const {
    join,
    map,
    defaultTo,
    hasPath,
    find,
    whereEq,
    pipe,
    prop,
    path,
    add,
    mean,
    flip,
    curry,
    isNil,
    ifElse,
    isEmpty,
    lensPath,
    over,
    identity,
    flatten,
    mergeRight,
    head,
} = require("ramda");
const { reduce: lodashreduce, map: lodashmap, size, isNaN, toNumber } = require("lodash");
let loreduce = curry((reducerFn, data) => lodashreduce(data, reducerFn));
let lomap = flip(lodashmap);
const { from } = require("rxjs");
const moment = require("moment");
const querystring = require("qs");

const { user_fields } = require("./utilities/fb_fields");

const request = async (payload) => {
    let { info, ...rest } = payload;
    try {
        let data = (response) => {
            if (hasPath(["data", "data"], response)) {
                return response.data.data;
            } else {
                return response.data;
            }
        };

        let res = await axios(rest);
        let next = path(["data", "paging", "next"], res);

        if (next) {
            return data(res).concat(await request({ method: "get", url: next }));
        } else {
            return data(res);
        }
    } catch (error) {
        console.log("facebookerror");
        let { message, status } = error.toJSON();
        // console.log(error.toJSON());
        console.log({ ...payload, message, status });

        return [];
        // console.log({ ...error.response.data.error });
    }
};

const User = (params) => {
    let { user_id, access_token } = params;

    const profile = {
        get: async (params) => {
            let url = `https://graph.facebook.com/v12.0/${user_id}`;
            let picture_url = `https://graph.facebook.com/v12.0/${user_id}/picture`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    fields: join(",", user_fields),
                    limit: 50,
                },
            };

            let picture_payload = {
                method: "get",
                url: picture_url,
                params: {
                    access_token,
                    redirect: false,
                },
            };

            let ad_acounts_response = await request(payload);
            let picture_response = await request(picture_payload);

            let { url: profile_picture_url } = picture_response;

            return {
                type: "object",
                data: { ...ad_acounts_response, profile_picture_url },
            };
        },
    };

    const ad_accounts = {
        get: async (params) => {
            let adaccount_fields = [
                "id",
                "account_id",
                "account_status",
                "age",
                "agency_client_declaration",
                "amount_spent",
                "attribution_spec",
                "balance",
                "business",
                "business_city",
                "business_country_code",
                "business_name",
                "business_state",
                "business_street",
                "business_street2",
                "business_zip",
                "can_create_brand_lift_study",
                "created_time",
                "currency",
                "disable_reason",
                "end_advertiser",
                "end_advertiser_name",
                "extended_credit_invoice_group",
                "failed_delivery_checks",
                "fb_entity",
                "funding_source_details",
                "has_advertiser_opted_in_odax",
                "has_migrated_permissions",
                "io_number",
                "is_attribution_spec_system_default",
                "is_direct_deals_enabled",
                "is_notifications_enabled",
                "is_personal",
                "is_prepay_account",
                "is_tax_id_required",
                "line_numbers",
                "media_agency",
                "min_campaign_group_spend_cap",
                "min_daily_budget",
                "name",
                "offsite_pixels_tos_accepted",
                "owner",
                "partner",
                "spend_cap",
                "tax_id",
                "tax_id_status",
                "tax_id_type",
                "timezone_id",
                "timezone_name",
                "timezone_offset_hours_utc",
                "tos_accepted",
                "user_tasks",
                "user_tos_accepted",
            ];

            let url = `https://graph.facebook.com/v12.0/${user_id}/adaccounts`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    fields: join(",", adaccount_fields),
                    limit: 50,
                },
            };

            let ad_acounts_response = await request(payload);

            return {
                type: "object",
                data: ad_acounts_response,
            };
        },
    };

    return {
        user_id,
        ad_accounts,
        profile,
    };
};

const Account = (params) => {
    let { fb_ad_account_id, access_token } = params;

    const to_path = (path, data) => {
        let lens_path = map((p) => {
            if (p.includes("*")) {
                let property = p.substring(1);
                return data[property];
            } else {
                return p;
            }
        }, path);
        return lens_path;
    };

    const metrics = (insight) => {
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
    };

    const stats = pipe(
        map(metrics),
        loreduce((prev, curr) => ({
            clicks: add(prev.clicks, curr.clicks),
            spend: add(prev.spend, curr.spend),
            made: add(prev.made, curr.made),
            sales: add(prev.sales, curr.sales),
            roas: mean([prev.roas, curr.roas]),
            leads: add(prev.leads, curr.leads),
        }))
    );

    const utilities = {
        is_abo: (adset) => {
            return adset.daily_budget ? true : false;
        },

        is_cbo: (campaign) => {
            // console.log("utilities:is_cbo");
            // console.log(campaign.daily_budget);
            // console.log(campaign);
            return campaign.daily_budget ? true : false;
        },
    };

    const business = {
        get: async (params) => {
            let func_name = "Facebook:Account:business:get";
            console.log(func_name);

            let url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}?fields=business`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    fields: join(",", ["business"]),
                },
            };

            let response = await request(payload);
            return response.business;
        },

        owned_pixels: async ({ business_id, fields = [] } = {}) => {
            let func_name = "Facebook:Account:business:owned_pixels";
            console.log(func_name);

            let url = `https://graph.facebook.com/v12.0/${business_id}/owned_pixels`;

            let payload_fields = isEmpty(fields) ? ["id"] : fields;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    fields: join(",", payload_fields),
                },
            };

            let response = await request(payload);
            return response;
        },

        ad_pixels: async ({ business_id, limit = 500, fields = [] } = {}) => {
            let func_name = "Facebook:Account:business:ad_pixels";
            console.log(func_name);

            console.log(business_id);

            let url = `https://graph.facebook.com/v12.0/${business_id}/adspixels`;

            let payload_fields = isEmpty(fields) ? ["id"] : fields;

            // let payload = {
            //     method: "get",
            //     url,
            //     params: {
            //         access_token,
            //         fields: join(",", payload_fields),
            //     },
            // };

            // let response = await request(payload);
            // console.log("response");
            // console.log(response);

            let response = await axios.get(url, { params: { access_token, limit, fields: join(",", payload_fields) } });

            return response.data;
        },

        owned_pages: async ({ business_id, fields = [] } = {}) => {
            let func_name = "Facebook:Account:business:owned_pages";
            console.log(func_name);

            let url = `https://graph.facebook.com/v12.0/${business_id}/owned_pages`;

            let payload_fields = isEmpty(fields) ? ["id"] : fields;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    fields: join(",", payload_fields),
                },
            };

            let response = await request(payload);
            return response;
        },
    };

    const creative = {
        create: async (params) => {
            let func_name = "Facebook:Account:creative:create";
            console.log(func_name);

            let url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}/adcreatives`;

            // let payload_fields = isEmpty(fields) ? ["id"] : fields;

            let { name, object_story_spec } = params;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    name,
                    object_story_spec,
                    // fields: join(",", payload_fields),
                },
            };

            let response = await request(payload);
            return response;
        },
    };

    const campaign_insights = {
        get: async (params) => {
            let insight_fields = [
                "account_id",
                "account_name",
                "action_values",
                "actions",
                "ad_click_actions",
                "ad_id",
                "ad_impression_actions",
                "ad_name",
                "adset_id",
                "adset_name",
                "age_targeting",
                "attribution_setting",
                "auction_bid",
                "auction_competitiveness",
                "auction_max_competitor_bid",
                "buying_type",
                "campaign_id",
                "campaign_name",
                "canvas_avg_view_percent",
                "canvas_avg_view_time",
                "catalog_segment_actions",
                "catalog_segment_value",
                "catalog_segment_value_mobile_purchase_roas",
                "catalog_segment_value_omni_purchase_roas",
                "catalog_segment_value_website_purchase_roas",
                "clicks",
                "conversion_values",
                "conversions",
                "converted_product_quantity",
                "converted_product_value",
                "cost_per_15_sec_video_view",
                "cost_per_2_sec_continuous_video_view",
                "cost_per_action_type",
                "cost_per_ad_click",
                "cost_per_conversion",
                "cost_per_dda_countby_convs",
                "cost_per_inline_link_click",
                "cost_per_inline_post_engagement",
                "cost_per_one_thousand_ad_impression",
                "cost_per_outbound_click",
                "cost_per_thruplay",
                "cost_per_unique_action_type",
                "cost_per_unique_click",
                "cost_per_unique_inline_link_click",
                "cost_per_unique_outbound_click",
                "cpc",
                "cpm",
                "cpp",
                "created_time",
                "ctr",
                "date_start",
                "date_stop",
                "dda_countby_convs",
                "dda_results",
                "estimated_ad_recall_rate_lower_bound",
                "estimated_ad_recall_rate_upper_bound",
                "estimated_ad_recallers_lower_bound",
                "estimated_ad_recallers_upper_bound",
                "frequency",
                "full_view_impressions",
                "full_view_reach",
                "gender_targeting",
                "impressions",
                "inline_link_click_ctr",
                "inline_link_clicks",
                "inline_post_engagement",
                "instant_experience_clicks_to_open",
                "instant_experience_clicks_to_start",
                "instant_experience_outbound_clicks",
                "interactive_component_tap",
                "labels",
                "location",
                "mobile_app_purchase_roas",
                "objective",
                "optimization_goal",
                "outbound_clicks",
                "outbound_clicks_ctr",
                "purchase_roas",
                "qualifying_question_qualify_answer_rate",
                "reach",
                "social_spend",
                "spend",
                "updated_time",
                "video_15_sec_watched_actions",
                "video_30_sec_watched_actions",
                "video_avg_time_watched_actions",
                "video_continuous_2_sec_watched_actions",
                "video_p100_watched_actions",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p95_watched_actions",
                "video_play_actions",
                "video_play_curve_actions",
                "video_play_retention_0_to_15s_actions",
                "video_play_retention_20_to_60s_actions",
                "video_play_retention_graph_actions",
                "video_time_watched_actions",
                "website_ctr",
                "website_purchase_roas",
                "wish_bid",
            ];

            let { campaign_id, time_range, fields = [] } = params;
            fields = isEmpty(fields) ? insight_fields : fields;
            let url = `https://graph.facebook.com/v12.0/${campaign_id}/insights`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    fields: join(",", fields),
                },
            };

            let insights = await request(payload);

            return { type: "object", data: { campaign_id, insights } };
        },
    };

    const adimages = {
        set: async (params) => {
            console.log("adimages:set");
            // console.log(params);

            let { bytes, name } = params;

            let url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}/adimages`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    bytes,
                    name,
                },
            };

            let result = await request(payload);

            return result;
        },
    };

    const campaign = {
        get: async ({ time_range, campaign_id, fields = [] } = {}) => {
            let func_name = "Facebook:Account:campaign:get";
            console.log(func_name);

            if (!time_range) return throwError(`error:${func_name}:no time_range`);
            if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);

            let campaign_fields = [
                "id",
                "account_id",
                "adlabels",
                "bid_strategy",
                "budget_remaining",
                "buying_type",
                "can_use_spend_cap",
                "configured_status",
                "created_time",
                "daily_budget",
                "effective_status",
                "is_skadnetwork_attribution",
                "issues_info",
                "last_budget_toggling_time",
                "lifetime_budget",
                "name",
                "objective",
                "pacing_type",
                "promoted_object",
                "recommendations",
                "smart_promotion_type",
                "source_campaign",
                "source_campaign_id",
                "special_ad_categories",
                "special_ad_category",
                "special_ad_category_country",
                "spend_cap",
                "start_time",
                "status",
                "stop_time",
                "topline_id",
                "updated_time",
                "boosted_object_id",
                "brand_lift_studies",
                "budget_rebalance_flag",
            ];

            campaign_fields = isEmpty(fields) ? campaign_fields : fields;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let payload = {
                info: { endpoint: "get", asset_type: "campaign", campaign_id },
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    fields: join(",", campaign_fields),
                },
            };

            let campaign = await request(payload);

            return {
                type: "object",
                data: { campaign_id, ...campaign },
            };
        },

        start: async (params) => {
            console.log("campaign:start");
            console.log(params);

            let { campaign_id, status } = params;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    status,
                },
            };

            let campaign = await request(payload);

            console.log("campaign:start:result");
            console.log(campaign);

            return {
                type: "object",
                data: { campaign_id, ...campaign },
            };
        },

        stop: async (params) => {
            console.log("campaign:stop");
            console.log(params);

            let { campaign_id, status } = params;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    status,
                },
            };

            // let campaign = await request(payload);

            // console.log("campaign:stop:result");
            // console.log(campaign);

            return {
                type: "object",
                data: { campaign_id, prev_value: "active", updated_value: "paused" },
            };
        },

        delete: async (params) => {
            console.log("campaign:delete");
            console.log(params);

            let { campaign_id } = params;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let payload = {
                method: "delete",
                url,
                params: {
                    access_token,
                },
            };

            let campaign = await request(payload);

            console.log("campaign:delete:result");
            console.log(campaign);

            return {
                type: "object",
                data: { campaign_id, ...campaign },
            };
        },

        set_budget: async (params) => {
            // console.log("campaign:set_budget");

            let { campaign_id, value } = params;
            let daily_budget = Number(value) * 100;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    daily_budget,
                },
            };

            let result = await request(payload);

            return {
                type: "object",
                data: { campaign_id, ...result },
            };
        },

        increase_budget: async (params) => {
            console.log("campaign:increase_budget");

            let { campaign_id, type, value, spend_cap, lifetime_budget, time_range } = params;
            let { since, until } = time_range;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let account = Account({ fb_ad_account_id, access_token });

            let campaign = await account.get({
                campaign: {
                    path: [],
                    params: {
                        time_range: { since, until },
                        campaign_id,
                    },
                },
            });

            if (account.utilities.is_cbo(campaign)) {
                if (type == "percent") {
                    let { daily_budget } = campaign;
                    let increase_amount = (Number(value) / 100) * Number(daily_budget);
                    let new_daily_budget = parseInt(Number(daily_budget) + increase_amount);

                    console.log("campaigns:increase_budget:percent");
                    console.log(access_token);
                    console.log(campaign);
                    console.log(campaign_id);
                    console.log(daily_budget);
                    console.log(increase_amount);
                    console.log(new_daily_budget);

                    let payload = {
                        info: { endpoint: "increase_budget", type: "percent", asset_type: "campaign" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);

                    return {
                        type: "object",
                        data: { campaign_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }

                if (type == "amount") {
                    console.log("updating_campaign_value");
                    let { daily_budget } = campaign;
                    let increase_amount = Number(value) * 100;
                    let new_daily_budget = parseInt(Number(daily_budget) + increase_amount);

                    let payload = {
                        info: { endpoint: "increase_budget", type: "amount", asset_type: "campaign" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);

                    return {
                        type: "object",
                        data: { campaign_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }
            } else {
                return {
                    type: "object",
                    data: {},
                };
            }
        },

        decrease_budget: async (params) => {
            console.log("campaign:decrease_budget");

            let { campaign_id, type, value, spend_cap, lifetime_budget, time_range } = params;
            let { since, until } = time_range;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let account = Account({ fb_ad_account_id, access_token });

            let campaign = await account.get({
                campaign: {
                    path: [],
                    params: {
                        time_range: { since, until },
                        campaign_id,
                    },
                },
            });

            if (account.utilities.is_cbo(campaign)) {
                if (type == "percent") {
                    let { daily_budget } = campaign;
                    let decrease_amount = (Number(value) / 100) * Number(daily_budget);
                    let new_daily_budget = Number(daily_budget) - decrease_amount;

                    let payload = {
                        info: { endpoint: "decrease_budget", type: "percent", asset_type: "campaign" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);
                    // console.log(result);

                    return {
                        type: "object",
                        data: { campaign_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }

                if (type == "amount") {
                    let { daily_budget } = campaign;
                    let decrease_amount = Number(value) * 100;
                    let new_daily_budget = Number(daily_budget) - decrease_amount;

                    let payload = {
                        info: { endpoint: "decrease_budget", type: "amount", asset_type: "campaign" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);
                    console.log(result);

                    return {
                        type: "object",
                        data: { campaign_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }
            } else {
                return {
                    type: "object",
                    data: {},
                };
            }
        },

        create: async (params) => {
            console.log("campaign:create");
            console.log(params);

            let { status, name, objective, special_ad_categories, buying_type, daily_budget, pacing_type, bid_strategy } = params;

            let url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}/campaigns`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    status,
                    name,
                    objective,
                    special_ad_categories,
                    buying_type,
                    daily_budget,
                    pacing_type,
                    bid_strategy,
                },
            };

            let campaign = await request(payload);

            console.log("campaign:create:result");
            console.log(campaign);

            return campaign;
        },

        update: async (params) => {
            console.log("campaign:update");
            console.log(params);

            let { status, name, objective, special_ad_categories, buying_type, daily_budget, pacing_type, bid_strategy, campaign_id } = params;

            let url = `https://graph.facebook.com/v12.0/${campaign_id}`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    status,
                    name,
                    objective,
                    special_ad_categories,
                    daily_budget,
                    bid_strategy,
                },
            };

            let campaign = await request(payload);

            console.log("campaign:update:result");
            console.log(campaign);

            return campaign;
        },
    };

    const campaigns = {
        get: async (params) => {
            let func_name = "Facebook:Account:campaigns:get";
            console.log(func_name);

            let { time_range, limit, effective_status = [], fields = [] } = params;

            let campaigns_fields = isEmpty(fields)
                ? [
                      "id",
                      "account_id",
                      "ad_strategy_group_id",
                      "ad_strategy_id",
                      "adlabels",
                      "bid_strategy",
                      "boosted_object_id",
                      "brand_lift_studies",
                      "budget_rebalance_flag",
                      "budget_remaining",
                      "buying_type",
                      "campaign_id",
                      "can_create_brand_lift_study",
                      "can_use_spend_cap",
                      "configured_status",
                      "created_time",
                      "daily_budget",
                      "effective_status",
                      "is_skadnetwork_attribution",
                      "issues_info",
                      "last_budget_toggling_time",
                      "lifetime_budget",
                      "name",
                      "objective",
                      "pacing_type",
                      "promoted_object",
                      "recommendations",
                      "smart_promotion_type",
                      "source_campaign",
                      "source_campaign_id",
                      "special_ad_categories",
                      "special_ad_category",
                      "special_ad_category_country",
                      "spend_cap",
                      "start_time",
                      "status",
                      "stop_time",
                      "topline_id",
                      "updated_time",
                  ]
                : fields;
            let campaigns_url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}/campaigns`;
            let limit_default = defaultTo(500);

            let info = { endpoint: "campaigns:get" };

            let payload = {
                info,
                method: "get",
                url: campaigns_url,
                params: {
                    access_token,
                    time_range,
                    limit: limit_default(limit),
                    effective_status,
                    fields: join(",", campaigns_fields),
                },
            };

            let campaigns = await request(payload);

            return { type: "array", data: map((campaign) => ({ ...campaign, campaign_id: campaign.id }), campaigns) };
        },
    };

    const adset_insights = {
        get: async (params) => {
            let insight_fields = [
                "account_currency",
                "account_id",
                "account_name",
                "action_values",
                "actions",
                "ad_click_actions",
                "ad_id",
                "ad_impression_actions",
                "ad_name",
                "adset_id",
                "adset_name",
                "age_targeting",
                "attribution_setting",
                "auction_bid",
                "auction_competitiveness",
                "auction_max_competitor_bid",
                "buying_type",
                "campaign_id",
                "campaign_name",
                "canvas_avg_view_percent",
                "canvas_avg_view_time",
                "catalog_segment_actions",
                "catalog_segment_value",
                "catalog_segment_value_mobile_purchase_roas",
                "catalog_segment_value_omni_purchase_roas",
                "catalog_segment_value_website_purchase_roas",
                "clicks",
                "conversion_values",
                "conversions",
                "converted_product_quantity",
                "converted_product_value",
                "cost_per_15_sec_video_view",
                "cost_per_2_sec_continuous_video_view",
                "cost_per_action_type",
                "cost_per_ad_click",
                "cost_per_conversion",
                "cost_per_dda_countby_convs",
                "cost_per_inline_link_click",
                "cost_per_inline_post_engagement",
                "cost_per_one_thousand_ad_impression",
                "cost_per_outbound_click",
                "cost_per_thruplay",
                "cost_per_unique_action_type",
                "cost_per_unique_click",
                "cost_per_unique_inline_link_click",
                "cost_per_unique_outbound_click",
                "cpc",
                "cpm",
                "cpp",
                "created_time",
                "ctr",
                "date_start",
                "date_stop",
                "dda_countby_convs",
                "dda_results",
                "estimated_ad_recall_rate_lower_bound",
                "estimated_ad_recall_rate_upper_bound",
                "estimated_ad_recallers_lower_bound",
                "estimated_ad_recallers_upper_bound",
                "frequency",
                "full_view_impressions",
                "full_view_reach",
                "gender_targeting",
                "impressions",
                "inline_link_click_ctr",
                "inline_link_clicks",
                "inline_post_engagement",
                "instant_experience_clicks_to_open",
                "instant_experience_clicks_to_start",
                "instant_experience_outbound_clicks",
                "interactive_component_tap",
                "labels",
                "location",
                "mobile_app_purchase_roas",
                "objective",
                "optimization_goal",
                "outbound_clicks",
                "outbound_clicks_ctr",
                "place_page_name",
                "purchase_roas",
                "qualifying_question_qualify_answer_rate",
                "reach",
                "social_spend",
                "spend",
                "updated_time",
                "video_15_sec_watched_actions",
                "video_30_sec_watched_actions",
                "video_avg_time_watched_actions",
                "video_continuous_2_sec_watched_actions",
                "video_p100_watched_actions",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p95_watched_actions",
                "video_play_actions",
                "video_play_curve_actions",
                "video_play_retention_0_to_15s_actions",
                "video_play_retention_20_to_60s_actions",
                "video_play_retention_graph_actions",
                "video_time_watched_actions",
                "website_ctr",
                "website_purchase_roas",
            ];

            let { adset_id, time_range, fields = [] } = params;
            fields = isEmpty(fields) ? insight_fields : fields;
            let url = `https://graph.facebook.com/v12.0/${adset_id}/insights`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    fields: join(",", fields),
                },
            };

            let insights = await request(payload);
            return { type: "object", data: { adset_id, insights } };
        },
    };

    const adset = {
        get: async (params) => {
            // console.log("adset");
            // console.log(params);
            let { adset_id, time_range, fields = [] } = params;

            let adset_fields = isEmpty(fields)
                ? [
                      "id",
                      "name",
                      "campaign_id",
                      "account_id",
                      "adlabels",
                      "adset_schedule",
                      "asset_feed_id",
                      "attribution_spec",
                      "bid_adjustments",
                      "bid_amount",
                      "bid_constraints",
                      "bid_info",
                      "bid_strategy",
                      "billing_event",
                      "budget_remaining",
                      "campaign",
                      "configured_status",
                      "created_time",
                      "creative_sequence",
                      "daily_budget",
                      "daily_min_spend_target",
                      "daily_spend_cap",
                      "destination_type",
                      "effective_status",
                      "end_time",
                      "frequency_control_specs",
                      "instagram_actor_id",
                      "is_dynamic_creative",
                      "issues_info",
                      "learning_stage_info",
                      "lifetime_budget",
                      "lifetime_imps",
                      "lifetime_min_spend_target",
                      "lifetime_spend_cap",
                      "multi_optimization_goal_weight",
                      "optimization_goal",
                      "optimization_sub_event",
                      "pacing_type",
                      "promoted_object",
                      "recommendations",
                      "recurring_budget_semantics",
                      "review_feedback",
                      "rf_prediction_id",
                      "source_adset",
                      "source_adset_id",
                      "start_time",
                      "status",
                      "targeting",
                      "targeting_optimization_types",
                      "time_based_ad_rotation_id_blocks",
                      "time_based_ad_rotation_intervals",
                      "updated_time",
                      "use_new_app_click",
                  ]
                : fields;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let payload = {
                info: { endpoint: "get", asset_type: "adset", adset_id },
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    fields: join(",", adset_fields),
                },
            };

            try {
                let result = await request(payload);

                return {
                    type: "object",
                    data: { adset_id, ...result },
                };
            } catch (error) {
                // console.log("errorrrrrr");
                return;
            }
        },

        start: async (params) => {
            // console.log("adset:start");
            // console.log(params);

            let { adset_id, status } = params;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    status,
                },
            };

            let adset = await request(payload);

            // console.log("adset:start:result");
            // console.log(adset);

            return {
                type: "object",
                data: { adset_id, ...adset },
            };
        },

        stop: async (params) => {
            console.log("adset:stop");
            console.log(params);

            let { adset_id, status } = params;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let payload = {
                info: { endpoint: "stop", asset_type: "adset" },
                method: "post",
                url,
                params: {
                    access_token,
                    status,
                },
            };

            let adset = await request(payload);

            // console.log("adset:start:result");
            // console.log(adset);

            return {
                type: "object",
                data: { adset_id, prev_value: "active", updated_value: "paused" },
            };
        },

        delete: async (params) => {
            console.log("adset:delete");
            console.log(params);

            let { adset_id } = params;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let payload = {
                method: "delete",
                url,
                params: {
                    access_token,
                },
            };

            let adset = await request(payload);

            console.log("adset:delete:result");
            console.log(adset);

            return {
                type: "object",
                data: { adset_id, ...adset, prev_value: adset.effective_status, updated_value: "deleted" },
            };
        },

        set_budget: async (params) => {
            // console.log("adset:set_budget");

            let { adset_id, value } = params;
            let daily_budget = Number(value) * 100;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    daily_budget,
                },
            };

            let result = await request(payload);

            return {
                type: "object",
                data: { adset_id, ...result },
            };
        },

        increase_budget: async (params) => {
            console.log("adset:increase_budget");
            console.log(params);

            let { adset_id, type, value, time_range } = params;
            let { since, until } = time_range;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let account = Account({ fb_ad_account_id, access_token });

            let adset = await account.get({
                adset: {
                    path: [],
                    params: {
                        time_range: { since, until },
                        adset_id,
                    },
                },
            });

            if (account.utilities.is_abo(adset)) {
                if (type == "percent") {
                    let { daily_budget } = adset;
                    let increase_amount = (Number(value) / 100) * Number(daily_budget);
                    let new_daily_budget = parseInt(Number(daily_budget) + increase_amount);

                    // console.log("adset:increase_budget:percent");
                    // console.log(access_token);
                    // console.log(adset);
                    // console.log(adset_id);
                    // console.log(daily_budget);
                    // console.log(increase_amount);
                    // console.log(new_daily_budget);

                    let payload = {
                        info: { endpoint: "increase_budget", type: "percent", asset_type: "adset" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);

                    return {
                        type: "object",
                        data: { adset_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }

                if (type == "amount") {
                    let { daily_budget } = adset;
                    let increase_amount = Number(value) * 100;
                    let new_daily_budget = parseInt(Number(daily_budget) + increase_amount);

                    let payload = {
                        info: { endpoint: "increase_budget", type: "amount", asset_type: "adset" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);

                    return {
                        type: "object",
                        data: { adset_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }
            } else {
                return {
                    type: "object",
                    data: {},
                };
            }
        },

        decrease_budget: async (params) => {
            console.log("adset:decrease_budget");
            console.log(params);

            let { adset_id, type, value, time_range } = params;
            let { since, until } = time_range;

            let url = `https://graph.facebook.com/v12.0/${adset_id}`;

            let account = Account({ fb_ad_account_id, access_token });

            let adset = await account.get({
                adset: {
                    path: [],
                    params: {
                        time_range: { since, until },
                        adset_id,
                    },
                },
            });

            if (account.utilities.is_abo(adset)) {
                if (type == "percent") {
                    let { daily_budget } = adset;
                    let decrease_amount = (Number(value) / 100) * Number(daily_budget);
                    let new_daily_budget = parseInt(Number(daily_budget) - decrease_amount);

                    let payload = {
                        info: { endpoint: "decrease_budget", type: "percent", asset_type: "adset" },
                        endpoint: "decrease_budget",
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);

                    return {
                        type: "object",
                        data: { adset_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }

                if (type == "amount") {
                    let { daily_budget } = adset;
                    let decrease_amount = Number(value) * 100;
                    let new_daily_budget = parseInt(Number(daily_budget) - decrease_amount);

                    let payload = {
                        info: { endpoint: "decrease_budget", type: "amount", asset_type: "adset" },
                        method: "post",
                        url,
                        params: {
                            access_token,
                            daily_budget: new_daily_budget,
                        },
                    };

                    let result = await request(payload);

                    return {
                        type: "object",
                        data: { adset_id, prev_value: daily_budget / 100, updated_value: new_daily_budget / 100 },
                    };
                }
            } else {
                return {
                    type: "object",
                    data: {},
                };
            }
        },

        create: async (params) => {
            console.log("adset:create");
            console.log(params);

            let { campaign_id, status, name, optimization_goal, billing_event, bid_amount, daily_budget, targeting, start_time } = params;

            let url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}/adsets`;

            let payload = {
                method: "post",
                url,
                params: {
                    access_token,
                    campaign_id,
                    status,
                    name,
                    optimization_goal,
                    billing_event,
                    bid_amount,
                    daily_budget,
                    targeting,
                    start_time: moment().toString(),
                },
            };

            let adset = await request(payload);

            console.log("adset:create:result");
            console.log(adset);

            return adset;
        },
    };

    const adsets = {
        get: async (params) => {
            let func_name = "Facebook:Account:adsets:get";
            console.log(func_name);

            let { campaign_id, time_range, limit, effective_status = [], fields = [] } = params;

            let adsets_fields = isEmpty(fields)
                ? [
                      "id",
                      "account_id",
                      "adset_name",
                      "adlabels",
                      "adset_schedule",
                      "asset_feed_id",
                      "attribution_spec",
                      "bid_adjustments",
                      "bid_amount",
                      "bid_constraints",
                      "bid_info",
                      "bid_strategy",
                      "billing_event",
                      "budget_remaining",
                      "campaign",
                      "campaign_name",
                      "campaign_id",
                      "configured_status",
                      "created_time",
                      "creative_sequence",
                      "daily_budget",
                      "daily_min_spend_target",
                      "daily_spend_cap",
                      "destination_type",
                      "effective_status",
                      "end_time",
                      "frequency_control_specs",
                      "instagram_actor_id",
                      "is_dynamic_creative",
                      "issues_info",
                      "learning_stage_info",
                      "lifetime_budget",
                      "lifetime_imps",
                      "lifetime_min_spend_target",
                      "lifetime_spend_cap",
                      "multi_optimization_goal_weight",
                      "name",
                      "optimization_goal",
                      "optimization_sub_event",
                      "pacing_type",
                      "promoted_object",
                      "recommendations",
                      "recurring_budget_semantics",
                      "review_feedback",
                      "rf_prediction_id",
                      "source_adset",
                      "source_adset_id",
                      "start_time",
                      "status",
                      "targeting",
                      "targeting_optimization_types",
                      "time_based_ad_rotation_id_blocks",
                      "time_based_ad_rotation_intervals",
                      "updated_time",
                      "use_new_app_click",
                  ]
                : fields;
            let url = `https://graph.facebook.com/v12.0/${campaign_id}/adsets`;
            let limit_default = defaultTo(500);

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    limit: limit_default(limit),
                    effective_status,
                    fields: join(",", adsets_fields),
                },
            };

            let result = await request(payload);
            return { type: "array", data: map((adset_value) => ({ ...adset_value, adset_id: adset_value.id }), result) };
        },
    };

    const ad_insights = {
        get: async (params) => {
            let insight_fields = [
                "account_currency",
                "account_id",
                "account_name",
                "action_values",
                "actions",
                "ad_click_actions",
                "ad_id",
                "ad_impression_actions",
                "ad_name",
                "adset_id",
                "adset_name",
                "age_targeting",
                "attribution_setting",
                "auction_bid",
                "auction_competitiveness",
                "auction_max_competitor_bid",
                "buying_type",
                "campaign_id",
                "campaign_name",
                "canvas_avg_view_percent",
                "canvas_avg_view_time",
                "catalog_segment_actions",
                "catalog_segment_value",
                "catalog_segment_value_mobile_purchase_roas",
                "catalog_segment_value_omni_purchase_roas",
                "catalog_segment_value_website_purchase_roas",
                "clicks",
                "conversion_values",
                "conversions",
                "converted_product_quantity",
                "converted_product_value",
                "cost_per_15_sec_video_view",
                "cost_per_2_sec_continuous_video_view",
                "cost_per_action_type",
                "cost_per_ad_click",
                "cost_per_conversion",
                "cost_per_dda_countby_convs",
                "cost_per_inline_link_click",
                "cost_per_inline_post_engagement",
                "cost_per_one_thousand_ad_impression",
                "cost_per_outbound_click",
                "cost_per_thruplay",
                "cost_per_unique_action_type",
                "cost_per_unique_click",
                // "cost_per_unique_conversion",
                "cost_per_unique_inline_link_click",
                "cost_per_unique_outbound_click",
                "cpc",
                "cpm",
                "cpp",
                "ctr",
                "date_start",
                "date_stop",
                "dda_countby_convs",
                "dda_results",
                "estimated_ad_recall_rate_lower_bound",
                "estimated_ad_recall_rate_upper_bound",
                "estimated_ad_recallers_lower_bound",
                "estimated_ad_recallers_upper_bound",
                "frequency",
                "full_view_impressions",
                "full_view_reach",
                "gender_targeting",
                "impressions",
                "inline_link_click_ctr",
                "inline_link_clicks",
                "inline_post_engagement",
                "instant_experience_clicks_to_open",
                "instant_experience_clicks_to_start",
                "instant_experience_outbound_clicks",
                "interactive_component_tap",
                "labels",
                "location",
                "mobile_app_purchase_roas",
                "objective",
                "optimization_goal",
                "outbound_clicks",
                "outbound_clicks_ctr",
                "place_page_name",
                "purchase_roas",
                "qualifying_question_qualify_answer_rate",
                "reach",
                "social_spend",
                "spend",
                "updated_time",
                "video_15_sec_watched_actions",
                "video_30_sec_watched_actions",
                "video_avg_time_watched_actions",
                "video_continuous_2_sec_watched_actions",
                "video_p100_watched_actions",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p95_watched_actions",
                "video_play_actions",
                "video_play_curve_actions",
                "video_play_retention_0_to_15s_actions",
                "video_play_retention_20_to_60s_actions",
                "video_play_retention_graph_actions",
                "video_time_watched_actions",
                "website_ctr",
                "website_purchase_roas",
                "wish_bid",
            ];

            let { ad_id, time_range, fields = [] } = params;
            fields = isEmpty(fields) ? insight_fields : fields;
            let url = `https://graph.facebook.com/v12.0/${ad_id}/insights`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    fields: join(",", fields),
                },
            };

            let insights = await request(payload);

            return { type: "object", data: { ad_id, insights } };
        },
    };

    const ad = {
        get: async (params) => {
            // console.log("adset");
            // console.log(params);
            let { ad_id, time_range, fields = [] } = params;

            let ad_fields = isEmpty(fields)
                ? [
                      "id",
                      "account_id",
                      "ad_review_feedback",
                      "adlabels",
                      "adset",
                      "adset_id",
                      "bid_amount",
                      "campaign",
                      "campaign_id",
                      "configured_status",
                      "conversion_domain",
                      "created_time",
                      "creative",
                      "effective_status",
                      "issues_info",
                      "last_updated_by_app_id",
                      "name",
                      "preview_shareable_link",
                      "recommendations",
                      "source_ad",
                      "source_ad_id",
                      "status",
                      "tracking_specs",
                      "updated_time",
                  ]
                : fields;

            let url = `https://graph.facebook.com/v12.0/${ad_id}`;

            let payload = {
                info: { endpoint: "get", asset_type: "ad", ad_id },
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    fields: join(",", ad_fields),
                },
            };

            try {
                let result = await request(payload);

                return {
                    type: "object",
                    data: { ad_id, ...result },
                };
            } catch (error) {
                // console.log("errorrrrrr");
                return;
            }
        },

        create: async (params) => {
            let func_name = "Facebook:Account:ad:create";
            console.log(func_name);

            let { name, adset_id, creative, status, tracking_specs } = params;

            let url = `https://graph.facebook.com/v12.0/act_${fb_ad_account_id}/ads`;
            let response = await axios.post(url, querystring.stringify({ name, adset_id, creative, status, tracking_specs, access_token }));

            return response.data;
        },
    };

    const ads = {
        get: async (params) => {
            let func_name = "Account:ads:get";
            console.log(func_name);

            let { adset_id, time_range, limit, effective_status = [], fields = [] } = params;

            let ads_fields = [
                "id",
                "account_id",
                "ad_review_feedback",
                "adlabels",
                "adset",
                "adset_id",
                "bid_amount",
                "campaign",
                "campaign_id",
                "configured_status",
                "conversion_domain",
                "created_time",
                "creative",
                "effective_status",
                "issues_info",
                "last_updated_by_app_id",
                "name",
                "preview_shareable_link",
                "recommendations",
                "source_ad",
                "source_ad_id",
                "status",
                "tracking_specs",
                "updated_time",
            ];

            ads_fields = isEmpty(fields) ? ads_fields : fields;
            let url = `https://graph.facebook.com/v12.0/${adset_id}/ads`;
            let limit_default = defaultTo(500);

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    time_range,
                    limit: limit_default(limit),
                    effective_status,
                    fields: join(",", ads_fields),
                },
            };

            let ads = await request(payload);
            return { type: "array", data: map((ad) => ({ ...ad, ad_id: ad.id }), ads) };
        },
    };

    const search = {
        query: async (params) => {
            console.log("search:query");

            let { q } = params;
            console.log(params);

            // https://graph.facebook.com/search?type=adinterest&q=[Golf]&limit=10000&locale=en_US&access_token=your-access-token

            let url = `https://graph.facebook.com/v12.0/search`;

            let payload = {
                method: "get",
                url,
                params: {
                    access_token,
                    q,
                    type: "adinterest",
                    limit: 10000,
                },
            };

            let result = await request(payload);

            return result;
        },
    };

    const get = async (spec) => {
        let payload = {};

        let resolve_spec = async (spec, prev) =>
            Promise.all(
                lomap(async (value, key) => {
                    let { params, path, ...next } = value;
                    let result;

                    if (!prev) {
                        result = await api[key]["get"](params);

                        if (result.type == "array") {
                            map((val) => {
                                payload = over(lensPath(to_path(path, val)), mergeRight(val), payload);
                            }, result.data);
                        }

                        if (result.type == "object") {
                            payload = over(lensPath(to_path(path, result.data)), mergeRight(result.data), payload);
                        }
                    } else {
                        if (prev.type == "object") {
                            result = await api[key]["get"]({ ...prev.data, ...params });

                            if (result.type == "array") {
                                map((val) => {
                                    payload = over(lensPath(to_path(path, val)), mergeRight(val), payload);
                                }, result.data);
                            }

                            if (result.type == "object") {
                                payload = over(lensPath(to_path(path, result.data)), mergeRight(result.data), payload);
                            }
                        }

                        if (prev.type == "array") {
                            result = await Promise.all(map((child) => api[key]["get"]({ ...child, ...params }), prev.data));
                            result = map((res) => res.data, result);
                            result = { type: "array", data: flatten(result) };

                            map((val) => {
                                payload = over(lensPath(to_path(path, val)), mergeRight(val), payload);
                            }, result.data);
                        }
                    }

                    if (!isEmpty(next)) {
                        if (size(next) > 1) {
                            await Promise.all(lomap(async (value, key) => await resolve_spec({ [key]: value }, result), next));
                        } else {
                            await resolve_spec(next, result);
                        }
                    } else {
                        return result;
                    }
                }, spec)
            );

        await resolve_spec(spec);
        return payload;
    };

    const post = async (spec) => {
        console.log("post");
        return await Promise.all(
            lomap(async (spec, endpoint) => {
                let { params } = spec;
                let { action } = params;

                return await api[endpoint][action](params);
            }, spec)
        );
    };

    let api = {
        business,
        creative,
        campaigns,
        campaign,
        campaign_insights,
        adsets,
        adset,
        adset_insights,
        ads,
        ad,
        ad_insights,
        adimages,
        search,
    };

    return {
        fb_ad_account_id,
        utilities,
        get,
        post,
    };
};

const Utilities = {
    is_valid_fb_id: (id) => !isNaN(toNumber(id)),
};

exports.Utilities = Utilities;
exports.Account = Account;
exports.User = User;
exports.request = request;
