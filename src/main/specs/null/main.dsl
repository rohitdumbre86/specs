xform using 'tokenList.xform' as tokenList
xform using "functionNames.xform" as params
xform using "entryTimeDsl.xform" as startTimeForAcquire
xform using "counter.xform" as counter
xform using "receiptsCount.xform" as receiptsCount
xform using "custom_tid.xform" as customtid

if (request_entities.customerinfo) do
    callapi /security/digital/v1/guid/bearer/inquiry_results?requestType=userGUID with method POST as customerInfo
    handleerror http_status | error_map_api_level
    with header "Authorization: Bearer {tokenList.tokens[0]}"
    with header "Accept:application/json"
    with header "Cache-Control: no-cache"
    with header "Content-Type: application/x-www-form-urlencoded"
    with header "X-AMEX-API-KEY: {provider_config.apiKey}"
    with header "X-AMEX-REQUEST-ID : {customtid.tid}"
    with FORM_URLENCODED body <<<
    >>>
    xform customerInfo using 'customer_info.xform' into rootentity.personalDatas
enddo