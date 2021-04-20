import com.google.gson.Gson;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intuit.platform.fdp.connectivity.xform.common.DataTransformRequest;
import com.intuit.platform.fdp.connectivity.xform.api.GenericDataTransformRequest;
import com.intuit.platform.fdp.connectivity.xform.api.GenericDataTransformResponse;
import com.intuit.platform.dataacquisition.acquire.common.GenericAcquireRequest;
import com.intuit.platform.dataacquisition.acquire.common.GenericAcquireResponse;
import com.intuit.platform.dataacquisition.acquire.common.validator.GenericEntityValidationSchema;
import com.intuit.platform.dataacquisition.acquire.common.validator.GenericEntityValidator;
import com.intuit.platform.dataacquisition.acquire.common.ConfigurationProperties;
//import com.intuit.platform.fdp.connectivity.oauth.api.ConfigurationProperties;
import com.intuit.platform.dataacquisition.entity.das.acquire.v1.ErrorType;
import com.intuit.platform.dataacquisition.entity.das.acquire.v2.AcquireRequest;
import com.intuit.platform.dataacquisition.entity.das.acquire.v2.Entity;
import com.intuit.platform.dataacquisition.entity.das.acquire.v2.ExecutionParams;
import com.intuit.platform.dataacquisition.entity.das.acquire.v2.ProviderResponse;
import com.intuit.platform.dataacquisition.entity.das.acquire.v2.RawHostData;
import com.intuit.platform.dataacquisition.entity.das.acquire.v2.HostResponse;
import com.intuit.platform.dataacquisition.entity.das.acquire.v1.DasError;
import com.intuit.platform.dataacquisition.provider.cachingclient.ProviderChannelDetail;
import com.intuit.platform.dataacquisition.acquire.common.exception.ConnectivityExceptionV2;
import com.intuit.platform.dataacquisition.acquire.common.ApiLevelErrorV2;
import com.intuit.platform.dataacquisition.acquire.common.AccountLevelStatusV2;
import com.intuit.platform.dataacquisition.acquire.common.exception.AcquireHostStatusExceptionV2;
import com.intuit.platform.dataacquisition.acquire.common.util.AcquireUtils;
import com.intuit.platform.dataacquisition.acquire.common.AccountSelectorV2;
import com.intuit.platform.dataacquisition.perflog.PerfThreadContext;
import com.intuit.platform.dataacquisition.perflog.PerfFields;
import com.intuit.platform.dataacquisition.perflog.PerfTimer;
import com.intuit.platform.dataacquisition.perflog.PerfLogEntry;
import com.intuit.platform.fdp.partnerauth.AuthHeaders;
import com.intuit.platform.fdp.partnerauth.AuthHeader;
import java.util.Properties;
import com.intuit.platform.fdp.connectivitydsl.runtime.DcsInputValidator;
import com.intuit.platform.fdp.connectivitydsl.runtime.ProviderResponseValidator;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.ConnectivityServiceNewI;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.ConnectAndTransformConnectivityServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.TransformConnectivityServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.AcquireConnectivityServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.OauthConnectivityServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.AdditionalParameters;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.AcquireData;
import com.intuit.platform.fdp.connectivitydsl.runtime.Chunking;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.EntityIdentificationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.XformConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ChunkingConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.PaginationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.CustomPaginationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.CustomErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ErrorCodeErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.HttpStatusErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ErrorCodeDefinition;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ERROR_HANDLER_TYPE;
import com.intuit.platform.fdp.connectivitydsl.runtime.ErrorHandler;
//import com.intuit.platform.fdp.connectivitydsl.runtime.InjectedErrorHandlerCode;
import com.intuit.platform.fdp.connectivitydsl.runtime.utils.Utils;
import com.intuit.platform.fdp.connectivitydsl.runtime.utils.DESUtil;
import com.intuit.platform.fdp.connectivitydsl.runtime.utils.DocServiceUtil;
import com.intuit.platform.fdp.connectivitydsl.runtime.utils.CMSUtil;
import com.intuit.platform.fdp.connectivitydsl.runtime.exception.DESException;
import com.intuit.platform.fdp.connectivitydsl.runtime.service.DcsInputService;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.DcsInputServiceNewI;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.transform.TransformDcsInputServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.transform.ConnectTransformDcsInputServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.acquire.AcquireDcsInputServiceNew;
import com.intuit.platform.fdp.connectivity.xform.api.GenericConnectAndTransformRequest;
import com.intuit.platform.fdp.connectivity.xform.api.GenericConnectAndTransformResponse;
import com.intuit.platform.fdp.connectivitydsl.runtime.DcsModifierDelegator;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.authUris.AuthUrisDcsInputServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.paTokens.PaTokensDcsInputServiceNew;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.deleteConnection.DeleteConnectionDcsInputServiceNew;
import com.intuit.platform.dataacquisition.acquire.common.reqsigner.RequestSigner;
import com.intuit.platform.fdp.connectivity.xform.common.DataTransformResponse;
import com.intuit.platform.fdp.connectivity.xform.exception.XformException;
import com.intuit.platform.fdp.connectivity.xform.common.XformError;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.json.JsonBuilder
import groovy.util.XmlSlurper

import org.apache.commons.codec.binary.Base64;

// Oauth flows
import com.intuit.platform.fdp.connectivity.oauth.api.GenericThirdPartyAuthUrisRequest;
import com.intuit.platform.fdp.connectivity.oauth.api.GenericThirdPartyOauthResponse;
import com.intuit.platform.fdp.connectivity.oauth.api.GenericThirdPartyPaTokensRequest;
import com.intuit.platform.fdp.connectivity.oauth.api.GenericThirdPartyDeleteConnectionRequest;
import com.intuit.platform.fdp.connectivity.oauth.exception.OauthException;
import com.intuit.platform.fdp.connectivity.oauth.common.OauthError;
//Send Data
import com.intuit.platform.fdp.connectivity.senddata.exception.ConnectAndTransformException;
import com.intuit.platform.fdp.connectivity.senddata.common.ConnectAndTransformError;

//Debug data
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.DebugLog;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.DebugInfo;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.DslArtifactInformation;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.DebugLogLevel;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.OperationLogType;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.OperationType;
import com.intuit.platform.fdp.connectivitydsl.runtime.helper.DebugHelper;
import com.intuit.platform.fdp.connectivitydsl.runtime.helper.DebugLogBuilder;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.debug.DebugParams;



public GenericAcquireResponse acquire(
    GenericAcquireRequest acquireRequest,
    HttpClient httpClient
) {
    PerfThreadContext.startTimer("groovy_processing_time");
    PerfThreadContext.set("dcs_spec_version",  "connectivity-dsl-acquire:0.51.0");
    String dslFileName = "main.dsl";
    String dslVersion = "connectivity-dsl-acquire:0.51.0";

    GenericAcquireResponse genericAcquireResponse = new GenericAcquireResponse();
    genericAcquireResponse.setHttpStatusCode(HttpStatus.SC_OK);

    String[][] unboundInputVariables = [
      ["request_entities", "customerinfo"], ["provider_config", "apiKey"]
    ]

    // will default to identity map
    def responseEntityNameToRequestEntityName = [
      "accounts": "account",
      "fdpEntities": "bankstatement"
    ]
    // will default to "primaryId"
    def requestEntityNameToPrimaryIdKey = [
      "account": "providerAccountId",
      "bankstatement": "providerAccountId"
    ]

    DcsInputServiceNewI dcsInputValidator = new AcquireDcsInputServiceNew(
                                                    acquireRequest, genericAcquireResponse, unboundInputVariables, responseEntityNameToRequestEntityName, requestEntityNameToPrimaryIdKey);
    Logger log = LoggerFactory.getLogger("dcs." + dcsInputValidator.getProviderId() + ".agent.groovy");
    ConfigurationProperties configurationProps = acquireRequest.getConfigurationProperties();
    if( configurationProps != null ) {
        Map<String, String> providerConfigurationFromWsi = configurationProps.getProviderConfiguration()
        if(providerConfigurationFromWsi != null && StringUtils.isNotEmpty(providerConfigurationFromWsi.get("fdp.wsi.request.signer")) ){
            log.info("Request signer from provider config: {}", providerConfigurationFromWsi.get("fdp.wsi.request.signer"));
            RequestSigner requestSigner = InjectedCode.callCustomRequestSignerBuilder(log, acquireRequest);
            acquireRequest.setRequestSigner(requestSigner);
            dcsInputValidator.setRequestSigner(requestSigner);

        }
    }

    if(configurationProps != null){
      log.debug("configurationProps {}", configurationProps);
    } else {
      log.debug("No configurationProps are received");
    }
    // set xform config
    XformConfig xformConfig = new XformConfig();
    xformConfig.setProviderId(dcsInputValidator.getProviderId());
    xformConfig.setConfigurationProps(configurationProps);
    xformConfig.setTokenizationService(acquireRequest.getTokenizationService());
    xformConfig.setDcsInputServiceNew(dcsInputValidator);

    if(acquireRequest.getAcquireRequestHeaders() != null) {
        xformConfig.setIntuitTidForInternalUse(acquireRequest.getOriginalIntuitTid());
        xformConfig.setOfferingId(acquireRequest.getAcquireRequestHeaders().getIntuitOfferingId());
        xformConfig.setIntuitTidForHostUse(acquireRequest.getAcquireRequestHeaders().getIntuitTid());
        xformConfig.setAuthorizationHeader(acquireRequest.getAcquireRequestHeaders().getAuthorization());
    }
    ProviderResponseValidator providerResponseValidator = new ProviderResponseValidator(dcsInputValidator.getProviderId());
    ConnectivityServiceNewI acquireSupport = new AcquireConnectivityServiceNew(dcsInputValidator.getProviderId(), dcsInputValidator, providerResponseValidator, configurationProps);
    List<Pattern> providerMaskingPatterns = null;
    InjectedCodeChunking injectedCodeChunkingImpl = null;
    ChunkingConfig chunkingConfig = null;
    List dateRangeChunkingParamsList = null;
    ErrorHandler errorHandler = null;
    AdditionalParameters additionalParameters = null;
    if (configurationProps != null && configurationProps.getMaskingPatterns() != null) {
        providerMaskingPatterns = configurationProps.getMaskingPatterns().get(dcsInputValidator.getProviderId());
    }

    String errorMapName;

    // Fields required for postprocess clause
    def postProcessImplicitArgs = [:];
    boolean stopPostProcessing = false;

    def accountFilters = [];
    boolean hasAccountEntity = false;
    boolean isRefreshRequest = false;

    String requestEntityName = null
    def totalEntityCounts = [:]

    if(acquireRequest.getAcquireRequestV2() != null &&
          acquireRequest.getAcquireRequestV2().getEntities() != null) {
        for(Entity entity : acquireRequest.getAcquireRequestV2().getEntities()) {
            // prepopulate totalEntityCounts for requested entities
            if (entity != null && entity.getEntityName() != null) {
                totalEntityCounts[entity.getEntityName().toUpperCase()] = 0
            }

            if (entity != null && StringUtils.containsIgnoreCase(entity.getEntityName(), "account")) {
                hasAccountEntity = true;

                def entityAccountFilters = AcquireUtils.fromAccountListMap(entity.getFilters());
                if(entityAccountFilters != null) {
                    accountFilters.addAll(entityAccountFilters);
                }
            }
            if (entity != null && entity.getDataLevel() != null && entity.getDataLevel().size() > 0 && entity.getDataLevel().contains("transactions")) {
                isRefreshRequest = true;
            }
        }
    }

    boolean returnNoAvailableAccounts = false;
    def returnNoAvailableAccountsString = dcsInputValidator.getProviderConfigProp("return.no.available.accounts");
    if(returnNoAvailableAccountsString != null && "true".equals(returnNoAvailableAccountsString)){
        returnNoAvailableAccounts = true;
    }
    String supportedAccountCategoriesString = dcsInputValidator.getProviderConfigProp("supported.account.categories");
    def supportedAccountCategories = (supportedAccountCategoriesString != null) ?
      supportedAccountCategoriesString.split("\\s*,\\s*") :
      null;

    def dcsResponse = [:]
    def rootEntity = [:]
    AcquireData acquireData = null;
    RawHostData rawHostData = null;
    def rawHostDataList = [];
    def rawHostDataListCompressed = [];
    if(acquireRequest.isIncludeRawData()) {
      dcsResponse["rawHostDataList"] = rawHostDataList
    }
    dcsResponse["providerId"] = dcsInputValidator.getProviderId();
    def json = [:]

    int totalAccountsCountFromHost = 0; // number of accounts attempting to xform
    int totalAccountsCountMapped = 0; // number of account xformed and inserted (valid accounts)
    int totalAccountsCountInserted = 0; // number of accounts inserted (valid accounts & accounts with bad status codes)
    int totalAccountsExcludedByAccountCategory = 0;
    int totalAccountsExcludedByAcquireFilter = 0;
    // totalAccountsCountFromHost = totalAccountsCountInserted + totalAccountsExcludedByAccountCategory + totalAccountsExcludedByAcquireFilter + (accounts in error status with no providerId)
    int totalTransactionCount = 0;
    int totalTransactionCalls = 0;
    def transactionCountByAccountMap = [:]
    def accountStatusByAccountMap = [:]
    int transactionCountByAccount = 0;
    boolean isPartialContent = false;
    final String ENTITY_ERROR_WITH_ADAPTER              = "needs adaptor fix (Issue identified with spec)"
    final String ENTITY_ERROR_WITH_PROVIDER_RESPONSE    = "needs provider response fix (Issue identified with provider response)"
    final String ENTITY_ERROR_PROVIDER_OUTAGE           = "intermittent provider outage (Outage in Provider systems - API 5xx)"
    final String ENTITY_ERROR_LOG_MSG_TEMPLATE          = "Error ({}) processing {} entity identified as {} from provider:{} - {}";
    final String ENTITY_EXCLUDE_NO_MATCHING_FILTER      = "did not match any request filters"
    final String ENTITY_EXCLUDE_NO_MATCHING_CATEGORY    = "did not match any entity categories"
    final String ENTITY_EXCLUDE_LOG_MSG_TEMPLATE        = "Excluding {} entity identified as {} from provider:{} - {}";
    //user to capture global provider/ script level errors. Will use above defined error types
    String dcsErrorType = null;
    EntityIdentificationConfig entityIdentificationConfig = null;

    InjectedPaginationCode injectedPaginationCodeObj = null;
    def customPaginationParamsList = new ArrayList();

    //field definition for retry
    def retryHandler = null;

    //field definition for error handlers
    List<String> attributesSubStrList = null;
    ErrorCodeDefinition errorCodeDefinition = null;
    ErrorHandlerConfig errorCodeErrorHandlerConfig = null;
    List<ErrorCodeDefinition> errorCodeDefinitionList = null;
    ErrorHandlerConfig httpStatusErrorHandlerConfig = null;
    ErrorHandlerConfig customErrorHandlerConfig = null;
    List modifierParamsGroovyVariableList = null;
    List<ErrorHandlerConfig> errorHandlerConfigList = null;

	// Variable for local debug
	boolean dcsDebugEnabled = System.getenv("DCS_DEBUG_ENABLED") || acquireRequest.isDebugEnabled();

    // entity level error variables
    ConnectivityExceptionV2 entityLevelException;
    def entityStatusCode = null;
    def entityStatusMessage = null;;
    def retainedKeysSet = null;
    def primaryId = null;
    def xformInsertionMap = [:]
    def xformInsertedSet = [] as Set;
    def xformSkippedSet = [] as Set;
    def booleanCondition = null;
    def xformPerfLogMap = [:]
    boolean passesFilter;
    boolean passesValidation;
    boolean isSupportedAccountCategory;
    GenericEntityValidationSchema schema = null;
    def dynamicMappingConfig = [:];

    // USER DEFINED VARS
    def customInjectedCodeVars = [:]

    // Fields required for callModifier
    def callModifierImplicitArgs = [:]
    def perfLogMaps = [:]
    perfLogMaps[ACCOUNTSTATUSBYACCOUNTMAP] = accountStatusByAccountMap
    perfLogMaps[TOTALENTITYCOUNTS] = totalEntityCounts
    perfLogMaps[TRANSACTIONCOUNTBYACCOUNTMAP] = transactionCountByAccountMap

    callModifierImplicitArgs[LOGGER] = log
    callModifierImplicitArgs[PERFLOGMAPS] = perfLogMaps

    // Fields required for custom error handling
    def customErrorHandlingImplicitArgs = [:]
    //def customAssertHandlingImplicitArgs = [:]
    def customRetryHandlingImplicitArgs = [:]
    customErrorHandlingImplicitArgs[INJECTED_CODE_VARS] = customInjectedCodeVars
    //customAssertHandlingImplicitArgs[INJECTED_CODE_VARS] = customInjectedCodeVars
    customRetryHandlingImplicitArgs[INJECTED_CODE_VARS] = customInjectedCodeVars

    callModifierImplicitArgs[INJECTED_CODE_VARS] = customInjectedCodeVars
    postProcessImplicitArgs[INJECTED_CODE_VARS] = customInjectedCodeVars

    //fields required for custom schema validation retrieval
    customSchemaValidationImplicitArgs = [:]
    customSchemaValidationImplicitArgs[INJECTED_CODE_VARS] = customInjectedCodeVars

    // Variable for insert statement
    def insertedExpression;

    // Variable to hold value for print statements
    def printOutput;

    // Set up headers
    def headers = [:] // from wsi
    def cookies = [] as Set // from wsi
    def requestHeaders = [:] // to call endpoints with

    def applicationPropertiesMap = acquireRequest.getProperties();
    boolean rawHostCollectionEnabled= false;
    if(applicationPropertiesMap){
        rawHostCollectionEnabled = Boolean.parseBoolean(applicationPropertiesMap.getOrDefault("rawhost.providerEnabled", "false"));
    }

    // Add all local variables needed for script into a map
    def scriptArguments = [:]
    scriptArguments["acquireRequest"] = acquireRequest
    scriptArguments["genericRequest"] = acquireRequest
    scriptArguments["genericResponse"] = genericAcquireResponse
    scriptArguments["httpClient"] = httpClient
    scriptArguments["log"] = log
    scriptArguments["configurationProps"] = configurationProps
    scriptArguments["dcsInputValidator"] = dcsInputValidator
    scriptArguments["entityIdentificationConfig"] = entityIdentificationConfig
    scriptArguments["xformConfig"] = xformConfig
    scriptArguments["providerResponseValidator"] = providerResponseValidator
    scriptArguments["acquireSupport"] = acquireSupport
    scriptArguments["providerMaskingPatterns"] = providerMaskingPatterns
    scriptArguments["injectedCodeChunkingImpl"] = injectedCodeChunkingImpl
    scriptArguments["chunkingConfig"] = chunkingConfig
    scriptArguments["dateRangeChunkingParamsList"] = dateRangeChunkingParamsList
    scriptArguments["errorHandler"] = errorHandler
    scriptArguments["additionalParameters"] = additionalParameters
    scriptArguments["errorMapName"] = errorMapName
    scriptArguments["accountFilters"] = accountFilters
    scriptArguments["requestEntityName"] = requestEntityName
    scriptArguments["totalEntityCounts"] = totalEntityCounts
    scriptArguments["supportedAccountCategories"] = supportedAccountCategories
    scriptArguments["rootEntity"] = rootEntity
    scriptArguments["acquireData"] = acquireData
    scriptArguments["rawHostData"] = rawHostData
    scriptArguments["rawHostDataList"] = rawHostDataList
    scriptArguments["rawHostDataListCompressed"] = rawHostDataListCompressed
    scriptArguments["json"] = json
    scriptArguments["totalAccountsCountFromHost"] = totalAccountsCountFromHost
    scriptArguments["totalAccountsCountMapped"] = totalAccountsCountMapped
    scriptArguments["totalAccountsCountInserted"] = totalAccountsCountInserted
    scriptArguments["totalAccountsExcludedByAccountCategory"] = totalAccountsExcludedByAccountCategory
    scriptArguments["totalAccountsExcludedByAcquireFilter"] = totalAccountsExcludedByAcquireFilter
    scriptArguments["totalTransactionCount"] = totalTransactionCount
    scriptArguments["totalTransactionCalls"] = totalTransactionCalls
    scriptArguments["transactionCountByAccountMap"] = transactionCountByAccountMap
    scriptArguments["accountStatusByAccountMap"] = accountStatusByAccountMap
    scriptArguments["transactionCountByAccount"] = transactionCountByAccount
    scriptArguments["ENTITY_ERROR_WITH_ADAPTER"] = ENTITY_ERROR_WITH_ADAPTER
    scriptArguments["ENTITY_ERROR_WITH_PROVIDER_RESPONSE"] = ENTITY_ERROR_WITH_PROVIDER_RESPONSE
    scriptArguments["ENTITY_ERROR_PROVIDER_OUTAGE"] = ENTITY_ERROR_PROVIDER_OUTAGE
    scriptArguments["ENTITY_ERROR_LOG_MSG_TEMPLATE"] = ENTITY_ERROR_LOG_MSG_TEMPLATE
    scriptArguments["ENTITY_EXCLUDE_NO_MATCHING_FILTER"] = ENTITY_EXCLUDE_NO_MATCHING_FILTER
    scriptArguments["ENTITY_EXCLUDE_NO_MATCHING_CATEGORY"] = ENTITY_EXCLUDE_NO_MATCHING_CATEGORY
    scriptArguments["ENTITY_EXCLUDE_LOG_MSG_TEMPLATE"] = ENTITY_EXCLUDE_LOG_MSG_TEMPLATE
    scriptArguments["dcsErrorType"] = dcsErrorType
    scriptArguments["injectedPaginationCodeObj"] = injectedPaginationCodeObj
    scriptArguments["customPaginationParamsList"] = customPaginationParamsList
    scriptArguments["attributesSubStrList"] = attributesSubStrList
    scriptArguments["errorCodeDefinition"] = errorCodeDefinition
    scriptArguments["errorCodeErrorHandlerConfig"] = errorCodeErrorHandlerConfig
    scriptArguments["errorCodeDefinitionList"] = errorCodeDefinitionList
    scriptArguments["httpStatusErrorHandlerConfig"] = httpStatusErrorHandlerConfig
    scriptArguments["customErrorHandlerConfig"] = customErrorHandlerConfig
    scriptArguments["modifierParamsGroovyVariableList"] = modifierParamsGroovyVariableList
    scriptArguments["errorHandlerConfigList"] = errorHandlerConfigList
    scriptArguments["entityLevelException"] = entityLevelException
    scriptArguments["entityStatusCode"] = entityStatusCode
    scriptArguments["entityStatusMessage"] = entityStatusMessage
    scriptArguments["retainedKeysSet"] = retainedKeysSet
    scriptArguments["primaryId"] = primaryId
    scriptArguments["xformInsertionMap"] = xformInsertionMap
    scriptArguments["xformInsertedSet"] = xformInsertedSet
    scriptArguments["xformSkippedSet"] = xformSkippedSet
    scriptArguments["booleanCondition"] = booleanCondition
    scriptArguments["xformPerfLogMap"] = xformPerfLogMap
    scriptArguments["passesFilter"] = passesFilter
    scriptArguments["passesValidation"] = passesValidation
    scriptArguments["isSupportedAccountCategory"] = isSupportedAccountCategory
    scriptArguments["schema"] = schema
    scriptArguments["callModifierImplicitArgs"] = callModifierImplicitArgs
    scriptArguments["perfLogMaps"] = perfLogMaps
    scriptArguments["postProcessImplicitArgs"] = postProcessImplicitArgs
    scriptArguments["stopPostProcessing"] = stopPostProcessing
    scriptArguments["customInjectedCodeVars"] = customInjectedCodeVars
    scriptArguments["customErrorHandlingImplicitArgs"] = customErrorHandlingImplicitArgs
    //scriptArguments["customAssertHandlingImplicitArgs"] = customErrorHandlingImplicitArgs
    scriptArguments["customRetryHandlingImplicitArgs"] = customRetryHandlingImplicitArgs
    scriptArguments["insertedExpression"] = insertedExpression
    scriptArguments["printOutput"] = printOutput
    scriptArguments["headers"] = headers
    scriptArguments["cookies"] = cookies
    scriptArguments["requestHeaders"] = requestHeaders
    scriptArguments["customSchemaValidationImplicitArgs"] = customSchemaValidationImplicitArgs
    scriptArguments["rawHostCollectionEnabled"] = rawHostCollectionEnabled
    scriptArguments["dcsDebugEnabled"] = dcsDebugEnabled;
    scriptArguments["retryHandler"] = retryHandler;
    scriptArguments["dynamicMappingConfig"] = dynamicMappingConfig;
    scriptArguments["dslFileName"] = dslFileName;
    scriptArguments["dslVersion"] = dslVersion;

    DslArtifactInformation dslArtifactInformation = null;
    DebugHelper debugHelper = null;
    DebugParams debugParams = null;
    if(dcsDebugEnabled){
        debugHelper = new DebugHelper();
        scriptArguments["debugHelper"] = debugHelper;
    }

    // Map to hold all the variables created to store some value
    def aesirVariables = [:]

    if (scriptArguments.acquireRequest.getAuthHeaders() != null) {
        // set up standard authorization header
        AuthHeader standardAuthHeader = scriptArguments.acquireRequest.getAuthHeaders().getStandard()
        if(standardAuthHeader != null && standardAuthHeader.getValue() != null) {
            String authHeaderString = "";
            if (standardAuthHeader.getValue().startsWith("Bearer") || standardAuthHeader.getValue().startsWith("Basic")) {
                authHeaderString = standardAuthHeader.getValue();
            } else {
                authHeaderString = "Bearer " + standardAuthHeader.getValue();
            }
            headers.put("Authorization", authHeaderString);
        }
        def customHeaderValue = "";
        def customHeaderKey = "";
        if(scriptArguments.acquireRequest.getAuthHeaders().getCustom() != null && !scriptArguments.acquireRequest.getAuthHeaders().getCustom().isEmpty()) {

            for (AuthHeader customAuthHeader: scriptArguments.acquireRequest.getAuthHeaders().getCustom()) {
                // set up cookie header if matches (ignore case) 'Cookie_{integer}'
                if (customAuthHeader.getName() != null && customAuthHeader.getValue() != null &&
                    customAuthHeader.getName().matches("(?i)(cookie_)(\\d+)")
                ) {
                    String cookieHeader = customAuthHeader.getValue().split(";")[0];
                    customHeaderKey = customAuthHeader.getName().replaceAll("_(\\d+)", "");
                    customHeaderValue = customHeaderValue.concat(cookieHeader).concat(";")
                    cookies.add(cookieHeader);

                } else {
                    // set up all other custom headers
                    headers.put(customAuthHeader.getName(), customAuthHeader.getValue())
                }
            }
        }
    }
    if(cookies != null && cookies.size() > 0) {
        scriptArguments.log.info("cookies received from PA {}",cookies);
    }
    try {
        // start of XFORM statement at line 1 and col 0, id="1_0"
        debugHelper = scriptArguments.debugHelper;
        dslArtifactInformation =null;
        debugParams = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 1 started");
            dslArtifactInformation = new DslArtifactInformation("1",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
        }
        // xform
        scriptArguments.xformInsertionMap["anonymous"] = xform('tokenList.xform', [], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
        scriptArguments["dynamicMappingConfig"] = null;
        // assign variable
        aesirVariables["aesir_0_tokenList"] = scriptArguments.xformInsertionMap["anonymous"];
        scriptArguments.providerResponseValidator.addExpr("aesir_0_tokenList", aesirVariables["aesir_0_tokenList"]);


        // Validate Entity Schema
        // Carry out post processing specified by the user using the postprocess clause
        scriptArguments.schema = null;
        if(scriptArguments.schema == null){
            scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("anonymous");
        }
        scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["anonymous"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
        if (!scriptArguments.passesValidation) {
          String errorDetails = "Xform failed for anonymous entity - failed schema validation";
          scriptArguments.log.debug(errorDetails);
          scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
          addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
          throw new ConnectivityExceptionV2(
            AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
            scriptArguments.dcsInputValidator.getProviderId(),
            errorDetails
          );
        }


        // Insert
        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 1 completed");
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
        }
        // end of XFORM statement at line 1 and col 0
        // start of XFORM statement at line 2 and col 0, id="2_0"
        debugHelper = scriptArguments.debugHelper;
        dslArtifactInformation =null;
        debugParams = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 2 started");
            dslArtifactInformation = new DslArtifactInformation("2",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
        }
        // xform
        scriptArguments.xformInsertionMap["anonymous"] = xform("functionNames.xform", [], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
        scriptArguments["dynamicMappingConfig"] = null;
        // assign variable
        aesirVariables["aesir_0_params"] = scriptArguments.xformInsertionMap["anonymous"];
        scriptArguments.providerResponseValidator.addExpr("aesir_0_params", aesirVariables["aesir_0_params"]);


        // Validate Entity Schema
        // Carry out post processing specified by the user using the postprocess clause
        scriptArguments.schema = null;
        if(scriptArguments.schema == null){
            scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("anonymous");
        }
        scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["anonymous"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
        if (!scriptArguments.passesValidation) {
          String errorDetails = "Xform failed for anonymous entity - failed schema validation";
          scriptArguments.log.debug(errorDetails);
          scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
          addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
          throw new ConnectivityExceptionV2(
            AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
            scriptArguments.dcsInputValidator.getProviderId(),
            errorDetails
          );
        }


        // Insert
        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 2 completed");
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
        }
        // end of XFORM statement at line 2 and col 0
        // start of XFORM statement at line 3 and col 0, id="3_0"
        debugHelper = scriptArguments.debugHelper;
        dslArtifactInformation =null;
        debugParams = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 3 started");
            dslArtifactInformation = new DslArtifactInformation("3",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
        }
        // xform
        scriptArguments.xformInsertionMap["anonymous"] = xform("entryTimeDsl.xform", [], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
        scriptArguments["dynamicMappingConfig"] = null;
        // assign variable
        aesirVariables["aesir_0_startTimeForAcquire"] = scriptArguments.xformInsertionMap["anonymous"];
        scriptArguments.providerResponseValidator.addExpr("aesir_0_startTimeForAcquire", aesirVariables["aesir_0_startTimeForAcquire"]);


        // Validate Entity Schema
        // Carry out post processing specified by the user using the postprocess clause
        scriptArguments.schema = null;
        if(scriptArguments.schema == null){
            scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("anonymous");
        }
        scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["anonymous"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
        if (!scriptArguments.passesValidation) {
          String errorDetails = "Xform failed for anonymous entity - failed schema validation";
          scriptArguments.log.debug(errorDetails);
          scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
          addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
          throw new ConnectivityExceptionV2(
            AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
            scriptArguments.dcsInputValidator.getProviderId(),
            errorDetails
          );
        }


        // Insert
        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 3 completed");
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
        }
        // end of XFORM statement at line 3 and col 0
        // start of XFORM statement at line 4 and col 0, id="4_0"
        debugHelper = scriptArguments.debugHelper;
        dslArtifactInformation =null;
        debugParams = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 4 started");
            dslArtifactInformation = new DslArtifactInformation("4",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
        }
        // xform
        scriptArguments.xformInsertionMap["anonymous"] = xform("counter.xform", [], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
        scriptArguments["dynamicMappingConfig"] = null;
        // assign variable
        aesirVariables["aesir_0_counter"] = scriptArguments.xformInsertionMap["anonymous"];
        scriptArguments.providerResponseValidator.addExpr("aesir_0_counter", aesirVariables["aesir_0_counter"]);


        // Validate Entity Schema
        // Carry out post processing specified by the user using the postprocess clause
        scriptArguments.schema = null;
        if(scriptArguments.schema == null){
            scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("anonymous");
        }
        scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["anonymous"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
        if (!scriptArguments.passesValidation) {
          String errorDetails = "Xform failed for anonymous entity - failed schema validation";
          scriptArguments.log.debug(errorDetails);
          scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
          addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
          throw new ConnectivityExceptionV2(
            AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
            scriptArguments.dcsInputValidator.getProviderId(),
            errorDetails
          );
        }


        // Insert
        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 4 completed");
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
        }
        // end of XFORM statement at line 4 and col 0
        // start of XFORM statement at line 5 and col 0, id="5_0"
        debugHelper = scriptArguments.debugHelper;
        dslArtifactInformation =null;
        debugParams = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 5 started");
            dslArtifactInformation = new DslArtifactInformation("5",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
        }
        // xform
        scriptArguments.xformInsertionMap["anonymous"] = xform("receiptsCount.xform", [], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
        scriptArguments["dynamicMappingConfig"] = null;
        // assign variable
        aesirVariables["aesir_0_receiptsCount"] = scriptArguments.xformInsertionMap["anonymous"];
        scriptArguments.providerResponseValidator.addExpr("aesir_0_receiptsCount", aesirVariables["aesir_0_receiptsCount"]);


        // Validate Entity Schema
        // Carry out post processing specified by the user using the postprocess clause
        scriptArguments.schema = null;
        if(scriptArguments.schema == null){
            scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("anonymous");
        }
        scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["anonymous"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
        if (!scriptArguments.passesValidation) {
          String errorDetails = "Xform failed for anonymous entity - failed schema validation";
          scriptArguments.log.debug(errorDetails);
          scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
          addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
          throw new ConnectivityExceptionV2(
            AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
            scriptArguments.dcsInputValidator.getProviderId(),
            errorDetails
          );
        }


        // Insert
        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 5 completed");
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
        }
        // end of XFORM statement at line 5 and col 0
        // start of XFORM statement at line 6 and col 0, id="6_0"
        debugHelper = scriptArguments.debugHelper;
        dslArtifactInformation =null;
        debugParams = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 6 started");
            dslArtifactInformation = new DslArtifactInformation("6",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
        }
        // xform
        scriptArguments.xformInsertionMap["anonymous"] = xform("custom_tid.xform", [], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
        scriptArguments["dynamicMappingConfig"] = null;
        // assign variable
        aesirVariables["aesir_0_customtid"] = scriptArguments.xformInsertionMap["anonymous"];
        scriptArguments.providerResponseValidator.addExpr("aesir_0_customtid", aesirVariables["aesir_0_customtid"]);


        // Validate Entity Schema
        // Carry out post processing specified by the user using the postprocess clause
        scriptArguments.schema = null;
        if(scriptArguments.schema == null){
            scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("anonymous");
        }
        scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["anonymous"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
        if (!scriptArguments.passesValidation) {
          String errorDetails = "Xform failed for anonymous entity - failed schema validation";
          scriptArguments.log.debug(errorDetails);
          scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
          addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
          throw new ConnectivityExceptionV2(
            AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
            scriptArguments.dcsInputValidator.getProviderId(),
            errorDetails
          );
        }


        // Insert
        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Acquire Xform at line 6 completed");
            addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
        }
        // end of XFORM statement at line 6 and col 0
        // start of IFELSE statement at line 8 and col 0, id="8_0"
        acquire_ifElseMethod_8_0(scriptArguments, aesirVariables)
        // end of IFELSE statement at line 8 and col 0
        // start of PRINT statement at line 22 and col 0, id="22_0"
        debugHelper = scriptArguments.debugHelper;
        debugParams = null;
        dslArtifactInformation = null;

        if(scriptArguments.dcsDebugEnabled) {
        	scriptArguments.log.info("Print at line 22 started");
            dslArtifactInformation = new DslArtifactInformation("22",
                                                                scriptArguments.dslFileName,
                                                                scriptArguments.dslVersion);

            debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
            addInfoDebugLog( debugParams, OperationType.PRINT, "Print started" );

        }
        scriptArguments.printOutput = "" + ;
        println("# " + scriptArguments.printOutput);
        println();
        scriptArguments.log.debug("dcs.print: " + scriptArguments.printOutput);
        if(scriptArguments.dcsDebugEnabled) {
            addInfoDebugLog( debugParams, OperationType.PRINT, "dcs.print: " + scriptArguments.printOutput );

        	scriptArguments.log.info("Print at line 22 completed");
            addInfoDebugLog( debugParams, OperationType.PRINT, "Print completed" );
        }
        // end of PRINT statement at line 22 and col 0

        if(scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS].containsKey(HTTP_STATUS) &&
                scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS].containsKey(BOOLEAN_OVERRIDE_RESPONSE_BODY) ){

            if(scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS] &&
                    scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][BOOLEAN_OVERRIDE_RESPONSE_BODY] == true){

                try {
                   def httpStatusCode = scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS].toInteger();
                   def responseBody = scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][BOOLEAN_OVERRIDE_RESPONSE_BODY];
                   genericAcquireResponse.setHttpStatusCode(httpStatusCode);

                   dcsResponse << scriptArguments.rootEntity;
                   genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.rootEntityToResponseString(dcsResponse));

                   genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);

                    //Perf logging for specific override scenario.
                    //Here two values can be overridden namely:
                    // scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][INTEGER_OVERRIDE_TOTAL_ACCOUNTS_COUNT_INSERTED]
                    // scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][INTEGER_OVERRIDE_TOTAL_TRANSACTION_COUNT]

                    PerfThreadContext.set("total_entity_counts_returned", scriptArguments.totalEntityCounts);

                    PerfThreadContext.set(PerfFields.total_accounts_count_from_host, scriptArguments.totalAccountsCountFromHost);
                    PerfThreadContext.set(PerfFields.total_accounts_count_mapped, scriptArguments.totalAccountsCountMapped);

                    PerfThreadContext.set(PerfFields.total_accounts_count_returned, scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][INTEGER_OVERRIDE_TOTAL_ACCOUNTS_COUNT_INSERTED]);
                    PerfThreadContext.set(PerfFields.total_transaction_count, scriptArguments.callModifierImplicitArgs[INJECTED_CODE_VARS][INTEGER_OVERRIDE_TOTAL_TRANSACTION_COUNT]);

                    PerfThreadContext.set("transaction_request_no_of_calls_made", scriptArguments.totalTransactionCalls);
                    PerfThreadContext.set(PerfFields.transaction_count_by_account, scriptArguments.transactionCountByAccountMap.toString());
                    PerfThreadContext.set("account_level_errors_service", scriptArguments.accountStatusByAccountMap.toString())
                    // collect response
                    scriptArguments.log.debug("Acquisition and Transformation for spec completed");
                    scriptArguments.log.info("Request accountFilters-{} accountsExculdedByFilter-{} isPartialContent-{}", scriptArguments.accountFilters.size(), scriptArguments.totalAccountsExcludedByAcquireFilter, isPartialContent);
                    PerfThreadContext.set(PerfFields.error_code_service,"0");
                    PerfThreadContext.set(PerfFields.error_msg_service,"OK");
                    PerfThreadContext.stopTimer("groovy_processing_time");

                    if(scriptArguments.dcsDebugEnabled) {
                        generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
                    }
                   return genericAcquireResponse;

                } catch(Exception e){
                   connectivityExceptionV2 = new ConnectivityExceptionV2(ApiLevelErrorV2.GENERAL_ERROR_101, scriptArguments.dcsInputValidator.getProviderId(), "Error with using custom code for httpStatus");
                   log.info("Acquire flow, issue found while casting callModifierImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS] to an integer {}", connectivityExceptionV2);
                   genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, connectivityExceptionV2));
                   genericAcquireResponse.setHttpStatusCode(connectivityExceptionV2.getHttpStatus());
                   genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
                    if(scriptArguments.dcsDebugEnabled) {
                        generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
                    }
                   return genericAcquireResponse;
                }
            }

        }


    } catch (ConnectivityExceptionV2 e) {
        ConnectivityExceptionV2 apiLevelException = (e.isApiLevelException()) ?
          e :
          new ConnectivityExceptionV2(
            ApiLevelErrorV2.HOST_NOT_AVAILABLE_105,
            scriptArguments.dcsInputValidator.getProviderId(),
            "Obtained non-api level error code: " + e.getFdpError().getCode()
          );

        if(scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS].containsKey(HTTP_STATUS)
            && scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS].containsKey(BOOLEAN_OVERRIDE_RESPONSE_BODY) ){
          if(scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS]
                && scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][BOOLEAN_OVERRIDE_RESPONSE_BODY] == true){
            try {
               def httpStatusCode = scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS].toInteger();
              dcsResponse << scriptArguments.rootEntity;

               genericAcquireResponse.setHttpStatusCode(httpStatusCode);
               genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.rootEntityToResponseString(dcsResponse) );
               genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
                if(scriptArguments.dcsDebugEnabled) {
                    generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
                }
               return genericAcquireResponse;
            } catch(Exception ex1){
              connectivityExceptionV2 = new ConnectivityExceptionV2(ApiLevelErrorV2.GENERAL_ERROR_101, scriptArguments.dcsInputValidator.getProviderId(), "Error with using custom code for httpStatus");
              log.info("Acquire flow, issue found while casting customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS] to an integer {}", connectivityExceptionV2);
               genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, connectivityExceptionV2));
               genericAcquireResponse.setHttpStatusCode(connectivityExceptionV2.getHttpStatus());
               genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
                if(scriptArguments.dcsDebugEnabled) {
                    generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
                }
               return genericAcquireResponse;
            }
          }
        }

        genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, apiLevelException));
        genericAcquireResponse.setHttpStatusCode(apiLevelException.getHttpStatus());
        genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);

        if (scriptArguments.dcsErrorType == null ) {
            scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_PROVIDER_OUTAGE;
        }
        // TODO: Is this the correct log message for here?
        scriptArguments.log.error(scriptArguments.ENTITY_ERROR_LOG_MSG_TEMPLATE,355,"accounts","UNKNOWN",scriptArguments.dcsInputValidator.getProviderId(),scriptArguments.dcsErrorType);
        if(scriptArguments.dcsDebugEnabled) {
            generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
        }
        return genericAcquireResponse;
    } finally {
		PerfThreadContext.set(PerfFields.total_call_count_to_provider, scriptArguments.acquireSupport.getCallCount());
    }

    PerfThreadContext.set("total_entity_counts_returned", scriptArguments.totalEntityCounts);

    PerfThreadContext.set(PerfFields.total_accounts_count_from_host, scriptArguments.totalAccountsCountFromHost);
    PerfThreadContext.set(PerfFields.total_accounts_count_mapped, scriptArguments.totalAccountsCountMapped);
        PerfThreadContext.set(PerfFields.total_accounts_count_returned, scriptArguments.totalAccountsCountInserted);
        PerfThreadContext.set(PerfFields.total_transaction_count, scriptArguments.totalTransactionCount);
    PerfThreadContext.set("transaction_request_no_of_calls_made", scriptArguments.totalTransactionCalls);
    PerfThreadContext.set(PerfFields.transaction_count_by_account, scriptArguments.transactionCountByAccountMap.toString());
    PerfThreadContext.set("account_level_errors_service", scriptArguments.accountStatusByAccountMap.toString())
    // collect response
    scriptArguments.log.debug("Acquisition and Transformation for spec completed");
    scriptArguments.log.info("Request accountFilters-{} accountsExculdedByFilter-{} isPartialContent-{}", scriptArguments.accountFilters.size(), scriptArguments.totalAccountsExcludedByAcquireFilter, isPartialContent);

    // TODO: will need to change implementation for existing accounts support
    // TODO: what about scenario where there are 2 filters, and 2 accounts that match the first but none that match the second
    // We didn't find all filters or error occurred so set partial content http status
    if(scriptArguments.rootEntity != null && scriptArguments.rootEntity.size() > 0 && scriptArguments.rootEntity.get("accounts") != null && !scriptArguments.rootEntity.get("accounts").isEmpty()) {
        def transactionTransientErrorCount = 0;
        scriptArguments.rootEntity.get("accounts").each{account ->
            if (account instanceof Map) {
              transactionTransientErrorCount += account.count { key, value ->
                  "statusCode".equals(key) && AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355.getFdpError().getCode().equals(value);
              }
            }
        }
        if(transactionTransientErrorCount > 0){
            isPartialContent = true;
        }
    }
    if (hasAccountEntity && scriptArguments.totalAccountsCountFromHost > 0 && scriptArguments.totalAccountsCountFromHost == scriptArguments.totalAccountsExcludedByAccountCategory) {
      // if we got more than 1 account from provider, but excluded them all because of the supported account categories
      if(isRefreshRequest){
        emptyAccountList = []
        dcsResponse["accounts"] = emptyAccountList
        genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.rootEntityToResponseString(dcsResponse));
        genericAcquireResponse.setHttpStatusCode(HttpStatus.SC_OK);
        genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
      } else {
        ConnectivityExceptionV2 e = new ConnectivityExceptionV2(ApiLevelErrorV2.NO_ENTITIES_FOR_PRESENCE_107, scriptArguments.dcsInputValidator.getProviderId(), "No entities found for this presence - supported account categories are: " + supportedAccountCategoriesString);
        genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, e));
        genericAcquireResponse.setHttpStatusCode(e.getHttpStatus());
        genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
      }

      if(scriptArguments.dcsDebugEnabled) {
            generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
      }
      return genericAcquireResponse;
    } else if(scriptArguments.accountFilters.size() > 0 && (scriptArguments.totalAccountsExcludedByAcquireFilter > 0 || scriptArguments.accountFilters.size() > scriptArguments.totalAccountsCountFromHost)) {
      //explicitly setting http status 200 for FI does not give us the account and We do not find match
      if(isPartialContent) {
         genericAcquireResponse.setHttpStatusCode(HttpStatus.SC_PARTIAL_CONTENT);
      } else {
        genericAcquireResponse.setHttpStatusCode(HttpStatus.SC_OK);
      }

      if(scriptArguments.totalAccountsCountInserted == 0){
        def emptyAccountsArray = [];
        scriptArguments.rootEntity.put("accounts", emptyAccountsArray)

      }
    } else if (hasAccountEntity && scriptArguments.totalAccountsCountInserted == 0) {
        if(scriptArguments.rootEntity.get("stageEntities") == null){
            ConnectivityExceptionV2 e = null;
            if(returnNoAvailableAccounts){
                e = new ConnectivityExceptionV2(ApiLevelErrorV2.NO_AVAILABLE_ACCOUNTS, scriptArguments.dcsInputValidator.getProviderId(), "All accounts delinquent");
            }
            else if(scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS].containsKey(HTTP_STATUS) && scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS].containsKey("isPartial")){
              if(scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS] && scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS]["isPartial"]){
                try {
                    def httpStatusCode = scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS].toInteger();
                    genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.rootEntityToResponseString(dcsResponse));
                    genericAcquireResponse.setHttpStatusCode(httpStatusCode);
                    genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
                    if(scriptArguments.dcsDebugEnabled) {
                        generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
                    }
                    return genericAcquireResponse;

                } catch(Exception ex){
                  connectivityExceptionV2 = new ConnectivityExceptionV2(ApiLevelErrorV2.GENERAL_ERROR_101, scriptArguments.dcsInputValidator.getProviderId(), "Error with using custom code for httpStatus");
                  log.info("Acquire flow, issue found while casting customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS] to an integer {}", connectivityExceptionV2);
                   genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, connectivityExceptionV2));
                   genericAcquireResponse.setHttpStatusCode(connectivityExceptionV2.getHttpStatus());
                   genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
                    if(scriptArguments.dcsDebugEnabled) {
                        generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
                    }
                   return genericAcquireResponse;

                }
              }
            }
             else {
                e = new ConnectivityExceptionV2(ApiLevelErrorV2.NO_CONSENTED_ACCOUNTS_378, scriptArguments.dcsInputValidator.getProviderId(), "No accounts found");
            }
          genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, e));
          genericAcquireResponse.setHttpStatusCode(e.getHttpStatus());
          genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
          if(scriptArguments.dcsDebugEnabled) {
            generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
          }
          return genericAcquireResponse;
      }
    } else if (isPartialContent) {
        //partial content case will happen in case of transaction validation failures
        genericAcquireResponse.setHttpStatusCode(HttpStatus.SC_PARTIAL_CONTENT);
    }
    if (scriptArguments.dcsInputValidator.getReservedVar("request_entity_domains", "tax") && (scriptArguments.rootEntity.get("documents") == null || scriptArguments.rootEntity.get("documents").isEmpty())) {
      ConnectivityExceptionV2 e = new ConnectivityExceptionV2(ApiLevelErrorV2.TAX_FORM_NOT_AVAILABLE_902, scriptArguments.dcsInputValidator.getProviderId(), "No documents available for tax year");
      genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, e));
      genericAcquireResponse.setHttpStatusCode(e.getHttpStatus());
      genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
      if(scriptArguments.dcsDebugEnabled) {
        generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
      }
      return genericAcquireResponse;
    }
    dcsResponse << scriptArguments.rootEntity;
    genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.rootEntityToResponseString(dcsResponse));
    genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
    PerfThreadContext.set(PerfFields.error_code_service,"0");
    PerfThreadContext.set(PerfFields.error_msg_service,"OK");
    PerfThreadContext.stopTimer("groovy_processing_time");
    // Here we are checking if user is seeking to manipulate httpStatusCode by using injected_code_vars map and checking that it contains httpStatus key.
    // We then set the httpStatusCode to what is given as the value.
    if(scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS].containsKey(HTTP_STATUS)){
      if(scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS]){
        try {
           def httpStatusCode = scriptArguments.customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS].toInteger();
           genericAcquireResponse.setHttpStatusCode(httpStatusCode);
        } catch(Exception e){
          connectivityExceptionV2 = new ConnectivityExceptionV2(ApiLevelErrorV2.GENERAL_ERROR_101, scriptArguments.dcsInputValidator.getProviderId(), "Error with using custom code for httpStatus");
          log.info("Acquire flow, issue found while casting customErrorHandlingImplicitArgs[INJECTED_CODE_VARS][HTTP_STATUS] to an integer {}", connectivityExceptionV2);
           genericAcquireResponse.setAcquireResponse(scriptArguments.acquireSupport.connectivityExceptionV2ToProviderLevelErrorString(dcsResponse, connectivityExceptionV2));
           genericAcquireResponse.setHttpStatusCode(connectivityExceptionV2.getHttpStatus());
           genericAcquireResponse.setRawHostDataList(rawHostDataListCompressed);
            if(scriptArguments.dcsDebugEnabled) {
                generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
            }
           return genericAcquireResponse;
        }
      }
    }

    if(scriptArguments.dcsDebugEnabled) {
        generateDebugInfo(debugHelper, PerfThreadContext.get(), "acquire", genericAcquireResponse);
    }

    return genericAcquireResponse;
}

DebugInfo debugInfo = null;

public DebugInfo getDebugInfo() {
    return debugInfo;
}

private AdditionalParameters updateAdditionalParametersForCallApiDebug(
                                        Logger log,
                                        AdditionalParameters additionalParameters,
                                        DebugParams debugParams,
                                        String debugMessage
                                        ){
    Boolean dcsDebugEnabled = null;
    DebugHelper debugHelper = null;
    DslArtifactInformation dslArtifactInformation = null;
    if(debugParams != null){
        dcsDebugEnabled = debugParams.getDebugEnabled();
        debugHelper = debugParams.getDebugHelper();
        dslArtifactInformation = debugParams.getDslArtifactInformation();
    }

    if(dcsDebugEnabled && log != null && debugHelper != null && dslArtifactInformation != null ) {

        log.info(debugMessage +"at line "+ dslArtifactInformation.getLineNumber());
        DebugLog debugLog = DebugLogBuilder.getInstance()
                                            .withDebugLogLevel(DebugLogLevel.INFO)
                                            .withMessage(debugMessage)
                                            .withLineNumber(dslArtifactInformation.getLineNumber())
                                            .withOperationType(OperationType.CALLAPI)
                                            .withFile(dslArtifactInformation.getDslFileName())
                                            .withOperationLogType(OperationLogType.PLATFORM)
                                            .build();

        debugHelper.addDebugLog(debugLog);
        if(additionalParameters == null){
            additionalParameters = new AdditionalParameters();
        }
        additionalParameters.setDslArtifactInformation(dslArtifactInformation);
        additionalParameters.setDebugEnabled(dcsDebugEnabled);
        additionalParameters.setDebugHelper(debugHelper);
    }

    return  additionalParameters;
}

private void generateDebugInfo(DebugHelper debugHelper, PerfLogEntry perfLogEntry,
                               String flow, Object responseObject){
    if(debugHelper != null){
        List<String> debugStatsKeys = null;
        String response = null;
        if(flow != null){
            if(flow.equals("acquire")){
                 debugStatsKeys = generateDebugStatsKeysForAcquire();
                 response = ( (GenericAcquireResponse)responseObject).getAcquireResponse();
            }
            else if(flow.equals("oauth")){
                debugStatsKeys = generateDebugStatsKeysForOauth();
                response = ( (GenericThirdPartyOauthResponse)responseObject).getTpOauthResponse();
            }
        }
        Utils.addStatsFromPerfLogs(debugHelper, perfLogEntry, debugStatsKeys);
        debugHelper.addResult(response);
        debugInfo = debugHelper.getDebugInfo();
    }
}

private List<String> generateDebugStatsKeysForAcquire(){
     List<String> debugStatsKeys = new ArrayList<>();
     debugStatsKeys.add("groovy_processing_time");
     debugStatsKeys.add("dcs_spec_version");
     debugStatsKeys.add("total_entity_counts_returned");
     debugStatsKeys.add(PerfFields.total_accounts_count_from_host.toString());
     debugStatsKeys.add(PerfFields.total_accounts_count_mapped.toString());
     debugStatsKeys.add(PerfFields.total_accounts_count_returned.toString());
     debugStatsKeys.add(PerfFields.total_transaction_count.toString());
     debugStatsKeys.add("transaction_request_no_of_calls_made");
     debugStatsKeys.add(PerfFields.transaction_count_by_account.toString());
     debugStatsKeys.add("account_level_errors_service");
     debugStatsKeys.add(PerfFields.error_code_service.toString());
     debugStatsKeys.add(PerfFields.error_msg_service.toString());
     debugStatsKeys.add(PerfFields.total_call_count_to_provider.toString());
     return debugStatsKeys;
}

private List<String> generateDebugStatsKeysForOauth(){
     List<String> debugStatsKeys = new ArrayList<>();
     debugStatsKeys.add("groovy_processing_time");
     debugStatsKeys.add("dcs_spec_version");
     debugStatsKeys.add("transaction_request_no_of_calls_made");
     debugStatsKeys.add(PerfFields.error_code_service.toString());
     debugStatsKeys.add(PerfFields.error_msg_service.toString());
     debugStatsKeys.add(PerfFields.total_call_count_to_provider.toString());
     return debugStatsKeys;
}

private void addInfoCallApiDebugLog(   DebugParams debugParams,
                                        String message  ) {
    Boolean dcsDebugEnabled = null;
    DebugHelper debugHelper = null;
    DslArtifactInformation dslArtifactInformation = null;
    if(debugParams != null){
        dcsDebugEnabled = debugParams.getDebugEnabled();
        debugHelper = debugParams.getDebugHelper();
        dslArtifactInformation = debugParams.getDslArtifactInformation();
    }

    if(dcsDebugEnabled != null && dcsDebugEnabled ){
        generateDebugLog(debugHelper, dslArtifactInformation, OperationType.CALLAPI, DebugLogLevel.INFO, OperationLogType.PLATFORM, message);
    }
}

private void addErrorCallApiDebugLog(   DebugParams debugParams,
                                        String message  ) {
    Boolean dcsDebugEnabled = null;
    DebugHelper debugHelper = null;
    DslArtifactInformation dslArtifactInformation = null;
    if(debugParams != null){
        dcsDebugEnabled = debugParams.getDebugEnabled();
        debugHelper = debugParams.getDebugHelper();
        dslArtifactInformation = debugParams.getDslArtifactInformation();
    }

    if(dcsDebugEnabled != null && dcsDebugEnabled ){
        generateDebugLog(debugHelper, dslArtifactInformation, OperationType.CALLAPI, DebugLogLevel.ERROR, OperationLogType.PLATFORM, message);
    }
}

private void addInfoDebugLog(   DebugParams debugParams,
                                OperationType operationType,
                                String message  ) {
    Boolean dcsDebugEnabled = null;
    DebugHelper debugHelper = null;
    DslArtifactInformation dslArtifactInformation = null;
    if(debugParams != null){
        dcsDebugEnabled = debugParams.getDebugEnabled();
        debugHelper = debugParams.getDebugHelper();
        dslArtifactInformation = debugParams.getDslArtifactInformation();
    }

    if(dcsDebugEnabled != null && dcsDebugEnabled ){
        generateDebugLog(debugHelper, dslArtifactInformation, operationType, DebugLogLevel.INFO, OperationLogType.PLATFORM, message);
    }
}

private void addErrorDebugLog(   DebugParams debugParams,
                                OperationType operationType,
                                String message  ) {
    Boolean dcsDebugEnabled = null;
    DebugHelper debugHelper = null;
    DslArtifactInformation dslArtifactInformation = null;
    if(debugParams != null){
        dcsDebugEnabled = debugParams.getDebugEnabled();
        debugHelper = debugParams.getDebugHelper();
        dslArtifactInformation = debugParams.getDslArtifactInformation();
    }

    if(dcsDebugEnabled != null && dcsDebugEnabled ){
        generateDebugLog(debugHelper, dslArtifactInformation, operationType, DebugLogLevel.ERROR, OperationLogType.PLATFORM, message);
    }
}


private void generateDebugLog( DebugHelper debugHelper,
                                DslArtifactInformation dslArtifactInformation,
                                OperationType operationType,
                                DebugLogLevel debugLogLevel,
                                OperationLogType operationLogType,
                                String message) {
    if(debugHelper != null && dslArtifactInformation != null){
        DebugLog debugLog = DebugLogBuilder.getInstance()
                                            .withDebugLogLevel(debugLogLevel)
                                            .withMessage(message)
                                            .withLineNumber(dslArtifactInformation.getLineNumber())
                                            .withOperationType(operationType)
                                            .withFile(dslArtifactInformation.getDslFileName())
                                            .withOperationLogType(operationLogType)
                                            .build();
        debugHelper.addDebugLog(debugLog);
    }
}



// TODO: tokenize, filter raw response
private void aggregateRawHostData(List rawHostDataList, AcquireData acquireData, List providerMaskingPatterns, List rawHostDataListCompressed) {
  if(acquireData.getRawHostData() != null) {
    acquireData.getRawHostData().setHostRequest(Utils.maskString(acquireData.getRawHostData().getHostRequest(), providerMaskingPatterns));
    acquireData.getRawHostData().setHostUrl(Utils.maskString(acquireData.getRawHostData().getHostUrl(), providerMaskingPatterns));
    acquireData.getRawHostData().setHostResponse(Utils.maskString(acquireData.getRawHostData().getHostResponse(), providerMaskingPatterns));
    if(!rawHostDataList.contains(acquireData.getRawHostData())) {
        rawHostDataList.add(acquireData.getRawHostData());
    }
    if( rawHostDataListCompressed != null){
        RawHostData rawHostDataCompressed = new RawHostData();
        rawHostDataCompressed.setHostRequest(acquireData.getRawHostData().getHostRequest());
        rawHostDataCompressed.setHostUrl(acquireData.getRawHostData().getHostUrl());
        rawHostDataCompressed.setHostResponse(Utils.getBase64EncodedGzippedContent(acquireData.getRawHostData().getHostResponse()));
        rawHostDataCompressed.setResponseContentType(acquireData.getRawHostData().getResponseContentType());
        rawHostDataCompressed.setChannelType(acquireData.getRawHostData().getChannelType());
        rawHostDataListCompressed.add(rawHostDataCompressed);
    }
  }
  for (AcquireData paginatedAcquireData: acquireData.getPaginatedData()) {
    aggregateRawHostData(rawHostDataList, paginatedAcquireData, providerMaskingPatterns, rawHostDataListCompressed);
  }
}

private void aggregateRawHostDataCompressed(List rawHostDataListCompressed, AcquireData acquireData, List providerMaskingPatterns) {
  if(acquireData.getRawHostData() != null) {
    acquireData.getRawHostData().setHostRequest(Utils.maskString(acquireData.getRawHostData().getHostRequest(), providerMaskingPatterns));
    acquireData.getRawHostData().setHostUrl(Utils.maskString(acquireData.getRawHostData().getHostUrl(), providerMaskingPatterns));
    acquireData.getRawHostData().setHostResponse(Utils.getBase64EncodedGzippedContent(Utils.maskString(acquireData.getRawHostData().getHostResponse(), providerMaskingPatterns)));
    if(!rawHostDataListCompressed.contains(acquireData.getRawHostData())) {
        rawHostDataListCompressed.add(acquireData.getRawHostData());
    }
  }
  for (AcquireData paginatedAcquireData: acquireData.getPaginatedData()) {
    aggregateRawHostDataCompressed(rawHostDataListCompressed, paginatedAcquireData, providerMaskingPatterns);
  }
}


// TODO: identify proper mechanism for masking responses for non-acquire flows
private void aggregateRawHostDataWithoutMasking(List rawHostDataList, AcquireData acquireData) {
  if(acquireData.getRawHostData() != null && !rawHostDataList.contains(acquireData.getRawHostData())) {
      rawHostDataList.add(acquireData.getRawHostData());
  }
  for (AcquireData paginatedAcquireData: acquireData.getPaginatedData()) {
    aggregateRawHostDataWithoutMasking(rawHostDataList, paginatedAcquireData, providerMaskingPatterns);
  }
}

// TODO: WSI-703: this needs to be generalized for all children entities
// for now try to get entity level mapping first
// if that fails, then try to get api level mapping
private ConnectivityExceptionV2 getErrorMappedConnectivityExceptionV2(String errorMapName, def errorValue, def log) {
    if (errorMapName != null && errorValue != null) {
        def firstErrorValue = (errorValue instanceof List) ? errorValue[0] : errorValue;
        log.debug("Mapping error code {} using {}", firstErrorValue, errorMapName);

        if (firstErrorValue != null) {
            PerfThreadContext.get().addField("error_code_host", String.valueOf(firstErrorValue));
            def fdpErrorCode = dcsModifierDelegator.call(errorMapName, "handleerror", firstErrorValue)
            log.info("mapped error code from {} to {}", firstErrorValue, fdpErrorCode);
            if (fdpErrorCode == null) {
                return null;
            }
            AccountLevelStatusV2 accountLevelStatusV2 = AccountLevelStatusV2.fromName(fdpErrorCode);
            log.debug("accountLevelStatusV2 {}", accountLevelStatusV2);
            if (accountLevelStatusV2 != null) {
                return new ConnectivityExceptionV2(accountLevelStatusV2);
            }

            ApiLevelErrorV2 apiLevelErrorV2 = ApiLevelErrorV2.fromName(fdpErrorCode);
            if (apiLevelErrorV2 != null) {
                return new ConnectivityExceptionV2(apiLevelErrorV2, null, "Bad response from provider");
            }
        }
    }
    return null;
}

private OauthException getErrorMappedOauthException(String errorMapName, def errorValue, def log) {
    if (errorMapName != null && errorValue != null) {
        def firstErrorValue = (errorValue instanceof List) ? errorValue[0] : errorValue;
        log.debug("Mapping error code {} using {}", firstErrorValue, errorMapName);

        if (firstErrorValue != null) {
            PerfThreadContext.get().addField("error_code_host", String.valueOf(firstErrorValue));
            def fdpErrorCode = dcsModifierDelegator.call(errorMapName, "handleerror", firstErrorValue)
            log.info("mapped error code from {} to {}", firstErrorValue, fdpErrorCode);
            if (fdpErrorCode == null) {
                return null;
            }

            OauthError oauthError = OauthError.fromCode(fdpErrorCode);
            if (oauthError != null) {
              return new OauthException(oauthError, null, "Bad response from provider");
            }
        }
    }
    return null;
}


private ConnectAndTransformException getErrorMappedConnectAndTransformException(String errorMapName, def errorValue, def log) {
    if (errorMapName != null && errorValue != null) {
        def firstErrorValue = (errorValue instanceof List) ? errorValue[0] : errorValue;
        log.debug("Mapping error code {} using {}", firstErrorValue, errorMapName);

        if (firstErrorValue != null) {
            PerfThreadContext.get().addField("error_code_host", String.valueOf(firstErrorValue));
            def fdpErrorCode = dcsModifierDelegator.call(errorMapName, "handleerror", firstErrorValue)
            log.info("mapped error code from {} to {}", firstErrorValue, fdpErrorCode);
            if (fdpErrorCode == null) {
                return null;
            }

            ConnectAndTransformError connectAndTransformError = ConnectAndTransformError.fromCode(fdpErrorCode);
            if (connectAndTransformError != null) {
                return new ConnectAndTransformException(connectAndTransformError);
            }
        }
    }
    return null;
}


private Object retrieveField(Map entityMap, String[] attributeFieldArr, String providerId){
    ApiLevelErrorV2 apiLevelErrorV2 = ApiLevelErrorV2.HOST_NOT_AVAILABLE_105;
    //dcsInputValidator.getProviderId()
    ConnectivityExceptionV2 connectivityExceptionV2 = new ConnectivityExceptionV2(apiLevelErrorV2, providerId, "Bad object or field mentioned incorrectly in spec");

    Object fieldObject = null;

    if(attributeFieldArr.size()  > 0){
        int fieldArrCntr = 0;
        fieldObject = entityMap.get(attributeFieldArr[fieldArrCntr]);
        fieldArrCntr++;
        if(fieldObject != null){
            while(fieldArrCntr <= attributeFieldArr.size() -1 ){
                if(fieldObject != null && fieldObject instanceof Map) {
                    fieldObject = fieldObject.get(attributeFieldArr[fieldArrCntr]);
                    fieldArrCntr++;
                }
                else if(fieldObject != null ){
                    throw connectivityExceptionV2;
                }
                else{
                    fieldObject = null;
                    break;
                }
            }
        }
    }
    else{
        throw connectivityExceptionV2;
    }

    return fieldObject;

}

private String[] retrieveFieldArray(String inputStr, String delimiter){
    String[] fieldArr;

    if(inputStr.indexOf(delimiter) != -1){
        fieldArr =inputStr.tokenize(delimiter);
    }
    else{
        fieldArr     = new String[1];
        fieldArr[0]  =  inputStr;
    }

    return fieldArr;
}

private def retrieveErrorValue(def log, def providerResponseValidator, def variable, String varName, String... path){
    def errorValue = null;
    try{
       errorValue = providerResponseValidator.getExprValue(variable, varName, path);
    } catch (ConnectivityExceptionV2 e) {
      log.error("Couldn't access the error field from provider response: {} - {}", variable, e.getMessage());
    }
    return errorValue;
}


  def acquire_ifElseMethod_8_0(def scriptArguments, def aesirVariables) {
      DslArtifactInformation dslArtifactInformation = null;
      DebugHelper debugHelper = null;
      DebugParams debugParams = null;

      if(scriptArguments.dcsDebugEnabled) {
          scriptArguments.log.info("Ifelse method at line 8 started");
          dslArtifactInformation = new DslArtifactInformation("8",
                                                            scriptArguments.dslFileName,
                                                            scriptArguments.dslVersion);
          debugHelper = scriptArguments.debugHelper;
          debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
          addInfoDebugLog( debugParams, OperationType.IF, "Ifelse statement started" );
      }
    scriptArguments.booleanCondition = null;
    try {
      scriptArguments.booleanCondition = ((scriptArguments.dcsInputValidator.getReservedVar(scriptArguments.entityIdentificationConfig, "request_entities", "customerinfo")));
    } catch (ConnectivityExceptionV2 e) {
      scriptArguments.log.debug("Failed to resolve condition expression due to acquire exception (defaulting to null): " + e.getMessage());
      addErrorDebugLog( debugParams, OperationType.IF, "Failed to resolve condition expression due to acquire exception (defaulting to null): " + e.getMessage() );
    } catch (Exception e) {
      scriptArguments.log.debug("Failed to resolve condition expression (defaulting to null): " + e.getMessage());
      addErrorDebugLog( debugParams, OperationType.IF, "Failed to resolve condition expression (defaulting to null): " + e.getMessage() );
    }

    if (scriptArguments.booleanCondition) {

          // start of CALLAPI statement at line 9 and col 4, id="9_4"
          scriptArguments.additionalParameters = null;
          debugHelper = scriptArguments.debugHelper;
          dslArtifactInformation =null;
          debugParams = null;
          if(scriptArguments.dcsDebugEnabled){
              dslArtifactInformation = new DslArtifactInformation("9",
                                                              scriptArguments.dslFileName,
                                                              scriptArguments.dslVersion);

              debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);

              scriptArguments.additionalParameters=
                              updateAdditionalParametersForCallApiDebug(
                                                          scriptArguments.log, scriptArguments.additionalParameters,
                                                          debugParams, "Acquire Callapi started" );
          }
          scriptArguments.acquireData = new AcquireData();
          scriptArguments.rawHostData = new RawHostData();
          scriptArguments.acquireData.setRawHostData(scriptArguments.rawHostData);

          try {
            scriptArguments.rawHostData.setHostUrl(scriptArguments.acquireSupport.normalizeUrl(scriptArguments.dcsInputValidator.getHostUrl(), "/security/digital/v1/guid/bearer/inquiry_results?requestType=userGUID"));
            scriptArguments.rawHostData.setHostRequest("");
            scriptArguments.acquireData.setHttpMethod("post");
            if(scriptArguments.cookies != null){
              scriptArguments.acquireData.setRequestCookies(scriptArguments.cookies);
            }
            scriptArguments.requestHeaders = scriptArguments.headers + ["Content-Type": "application/x-www-form-urlencoded"] + ["Authorization": "Bearer " + scriptArguments.providerResponseValidator.castExprToString(
          scriptArguments.providerResponseValidator.getExprValue(aesirVariables["aesir_0_tokenList"], "aesir_0_tokenList", "tokens[0]"), "aesir_0_tokenList.tokens[0]"), "Accept": "application/json", "Cache-Control": "no-cache", "Content-Type": "application/x-www-form-urlencoded", "X-AMEX-API-KEY": scriptArguments.providerResponseValidator.castExprToString(scriptArguments.providerResponseValidator.resolveString(scriptArguments.dcsInputValidator.getReservedVar("provider_config", "apiKey")), "provider_config.apiKey"), "X-AMEX-REQUEST-ID": scriptArguments.providerResponseValidator.castExprToString(
          scriptArguments.providerResponseValidator.getExprValue(aesirVariables["aesir_0_customtid"], "aesir_0_customtid", "tid"), "aesir_0_customtid.tid")]
            scriptArguments.acquireData.setRawRequestHeaders(scriptArguments.requestHeaders);
          } catch (ConnectivityExceptionV2 e) {
            scriptArguments.log.error("Error occurred forming acquire request for {}", Utils.maskString(scriptArguments.acquireData.rawHostData.getHostUrl(), scriptArguments.providerMaskingPatterns))
            scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_ADAPTER ;
            addErrorDebugLog( debugParams, OperationType.CALLAPI, "Error occurred forming acquire request" );
            throw e;
          }
          aesirVariables["aesir_1_customerInfo"] = null;
              scriptArguments.errorHandlerConfigList = new ArrayList<>();
                      scriptArguments.errorMapName = "error_map_api_level";

                          scriptArguments.httpStatusErrorHandlerConfig = new HttpStatusErrorHandlerConfig(scriptArguments.log, ERROR_HANDLER_TYPE.HTTP_STATUS, scriptArguments.errorMapName, scriptArguments.providerResponseValidator, dcsModifierDelegator,
                                                                                  false, false );
                          scriptArguments.errorHandlerConfigList.add(scriptArguments.httpStatusErrorHandlerConfig);

              scriptArguments.errorHandler = new InjectedErrorHandlerCode(scriptArguments.errorHandlerConfigList, scriptArguments.acquireSupport, scriptArguments.customErrorHandlingImplicitArgs);
              if(scriptArguments.additionalParameters == null){
                  scriptArguments.additionalParameters = new AdditionalParameters();
              }
              scriptArguments.additionalParameters.setErrorHandler(scriptArguments.errorHandler);

          try {
            scriptArguments.acquireData.setCallsCount(0);
            scriptArguments.json = scriptArguments.acquireSupport.callapi(scriptArguments.httpClient, scriptArguments.acquireData, (PaginationConfig) null, scriptArguments.entityIdentificationConfig, scriptArguments.additionalParameters);
            // TODO: WSI-703 - make entity agnostic
            if(StringUtils.containsIgnoreCase("aesir_1_customerInfo", "transaction")){
              scriptArguments.totalTransactionCalls += scriptArguments.acquireData.getCallsCount();
              scriptArguments.log.debug("totalTransactionCalls {} for url {} ", scriptArguments.totalTransactionCalls, Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns));
            }

            if (StringUtils.startsWithIgnoreCase(scriptArguments.acquireData.getRawRequestHeaders().get("Content-Type"), "application/pdf")) {
              scriptArguments.log.debug("Got a response for URL={} with httpStatusCode={}, response=INTENTIONALLY LEFT BLANK, and content type={}", Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns), scriptArguments.acquireData.getHttpStatusCode(), scriptArguments.acquireData.getRawRequestHeaders().get("Content-Type"));
            }
            else {
              scriptArguments.log.debug("Got a response for URL={} with httpStatusCode={} and json response={}", Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns), scriptArguments.acquireData.getHttpStatusCode(), Utils.getBase64EncodedGzippedContentBasedOnSize(Utils.maskString(scriptArguments.json, scriptArguments.providerMaskingPatterns), AcquireData.CHARLIMITFORGZIPPINGANDENCODING));
            }
             scriptArguments.log.info("includeRawData={} rawHostCollectionEnabled={}",scriptArguments.acquireRequest.isIncludeRawData(),scriptArguments.rawHostCollectionEnabled);
            if(scriptArguments.acquireRequest.isIncludeRawData()) {
              if(scriptArguments.rawHostCollectionEnabled){
                  aggregateRawHostData(scriptArguments.rawHostDataList, scriptArguments.acquireData, scriptArguments.providerMaskingPatterns, scriptArguments.rawHostDataListCompressed);
              } else {
                  aggregateRawHostData(scriptArguments.rawHostDataList, scriptArguments.acquireData, scriptArguments.providerMaskingPatterns, null);
              }
            } else if(scriptArguments.rawHostCollectionEnabled){
                 aggregateRawHostDataCompressed(scriptArguments.rawHostDataListCompressed, scriptArguments.acquireData, scriptArguments.providerMaskingPatterns);
            }
          } catch (ConnectivityExceptionV2 e) {
            scriptArguments.log.error("Error occurred during callapi for URL: {}", Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns));
              if(scriptArguments.acquireRequest.isIncludeRawData()) {
                if(scriptArguments.rawHostCollectionEnabled){
                    aggregateRawHostData(scriptArguments.rawHostDataList, scriptArguments.acquireData, scriptArguments.providerMaskingPatterns, scriptArguments.rawHostDataListCompressed);
                } else {
                    aggregateRawHostData(scriptArguments.rawHostDataList, scriptArguments.acquireData, scriptArguments.providerMaskingPatterns, null);
                }
              } else if(scriptArguments.rawHostCollectionEnabled){
                    aggregateRawHostDataCompressed(scriptArguments.rawHostDataListCompressed, scriptArguments.acquireData, scriptArguments.providerMaskingPatterns);
              }
            scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_PROVIDER_OUTAGE ;
            addErrorDebugLog( debugParams, OperationType.CALLAPI,
                                          "Error occurred during callapi for URL: "+Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns) );

            throw e;
          }
          aesirVariables["aesir_1_customerInfo"] = scriptArguments.json;
          scriptArguments.providerResponseValidator.addExpr("aesir_1_customerInfo", scriptArguments.json);

          // Carry out post processing specified by the user using the postprocess clause
          scriptArguments.stopPostProcessing = false;
          if (scriptArguments.acquireData.getHttpStatusCode() != HttpStatus.SC_OK) {
            scriptArguments.log.info("Got non 200 http status code={} - url={}", scriptArguments.acquireData.getHttpStatusCode(), Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns));
            // get list of other non 200 http status codes that we should log and continue parsing
            def okHttpStatuses = [];
            String okHttpStatusesString = scriptArguments.dcsInputValidator.getProviderConfigProp("provider.response.okHttpStatuses");
            if (okHttpStatusesString != null) {
              okHttpStatuses = okHttpStatusesString.split("\\s*,\\s*");
            }

            // if provider http status doesn't match create error
            if (scriptArguments.acquireData.getHttpStatusCode() == null || !okHttpStatuses.contains(scriptArguments.acquireData.getHttpStatusCode().toString())) {
               scriptArguments.log.error("Got a bad http status code {} when calling api {}", scriptArguments.acquireData.getHttpStatusCode(), scriptArguments.acquireData.toStringMasked(scriptArguments.providerMaskingPatterns));
               scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_PROVIDER_OUTAGE ;

              boolean stopErrorHandling = false;
              // TODO: simplify the logic below to utilize methods
              ConnectivityExceptionV2 connectivityExceptionV2 = null;

              def errorValue = null;
              boolean errorValueFound = false;

                  if (connectivityExceptionV2 == null && !stopErrorHandling) {
                    scriptArguments.errorMapName = "error_map_api_level";

                        errorValue = scriptArguments.acquireData.getHttpStatusCode().toString();
                        // TODO: WSI-703: this needs to be generalized for all children entities
                        // for now try to get entity level mapping first
                        // if that fails, then try to get api level mapping
                        connectivityExceptionV2 = getErrorMappedConnectivityExceptionV2(scriptArguments.errorMapName, errorValue, scriptArguments.log);
                   }
                    if (connectivityExceptionV2 != null){
                        addErrorDebugLog( debugParams, OperationType.CALLAPI,
                                                  "Error handled through error handler clause. " );
                        throw connectivityExceptionV2;
                    }



                  if(!stopErrorHandling){
                      addErrorDebugLog( debugParams, OperationType.CALLAPI,
                                                  "Error handling failed. Handling as GenericBadHttpStatus Error." );
                      throw scriptArguments.acquireSupport.getGenericBadHttpStatusException(scriptArguments.acquireData);
                  }
            } else {
              scriptArguments.log.debug("Continuing with http status code={} - url={}", scriptArguments.acquireData.getHttpStatusCode(), Utils.maskString(scriptArguments.acquireData.getRawHostData().getHostUrl(), scriptArguments.providerMaskingPatterns));
              addInfoDebugLog( debugParams, OperationType.CALLAPI,
                                       "Continuing with http status code="+scriptArguments.acquireData.getHttpStatusCode() );
            }

          }
          if(scriptArguments.dcsDebugEnabled) {
                  scriptArguments.log.info("Acquire Callapi at line 9 completed with httpcode: {}, body: {}", scriptArguments.acquireData.getHttpStatusCode(), scriptArguments.acquireData.getRawHostData().getHostResponse());
                  addInfoDebugLog( debugParams, OperationType.CALLAPI,
                                          "Acquire Callapi completed" );
          }
          // end of CALLAPI statement at line 9 and col 4
          // start of XFORM statement at line 19 and col 4, id="19_4"
          debugHelper = scriptArguments.debugHelper;
          dslArtifactInformation =null;
          debugParams = null;

          if(scriptArguments.dcsDebugEnabled) {
          	scriptArguments.log.info("Acquire Xform at line 19 started");
              dslArtifactInformation = new DslArtifactInformation("19",
                                                              scriptArguments.dslFileName,
                                                              scriptArguments.dslVersion);
              debugParams = new DebugParams(scriptArguments.dcsDebugEnabled, dslArtifactInformation, debugHelper);
              addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform started" );
          }
          // xform
          scriptArguments.xformInsertionMap["personalDatas"] = xform('customer_info.xform', ["customerInfo": aesirVariables["aesir_1_customerInfo"]], scriptArguments.xformConfig, scriptArguments.perfLogMaps,scriptArguments["dynamicMappingConfig"]);
          scriptArguments["dynamicMappingConfig"] = null;

          // Validate Entity Schema
          // Carry out post processing specified by the user using the postprocess clause
          scriptArguments.schema = null;
          if(scriptArguments.schema == null){
              scriptArguments.schema = (scriptArguments.configurationProps == null) ? null : scriptArguments.configurationProps.getEntityValidationSchemas().get("personalDatas");
          }
          scriptArguments.passesValidation = GenericEntityValidator.validateEntity(scriptArguments.schema, scriptArguments.xformInsertionMap["personalDatas"], scriptArguments.dcsInputValidator.safeGetReservedVar("start_date"), scriptArguments.dcsInputValidator.safeGetReservedVar("end_date"));
          if (!scriptArguments.passesValidation) {
            String errorDetails = "Xform failed for personalDatas entity - failed schema validation";
            scriptArguments.log.debug(errorDetails);
            scriptArguments.dcsErrorType = scriptArguments.ENTITY_ERROR_WITH_PROVIDER_RESPONSE;
            addErrorDebugLog( debugParams, OperationType.XFORM, errorDetails );
            throw new ConnectivityExceptionV2(
              AccountLevelStatusV2.ACCOUNT_AUTOMATICALLY_RESOLVE_355,
              scriptArguments.dcsInputValidator.getProviderId(),
              errorDetails
            );
          }


          // Insert
          scriptArguments.providerResponseValidator.safeGetExprValueAsCollection(scriptArguments.rootEntity, "rootEntity", "personalDatas").add(scriptArguments.xformInsertionMap["personalDatas"]);
          scriptArguments.xformInsertedSet.add("personalDatas");
          scriptArguments.log.trace("personalDatas element added to response");

          scriptArguments.requestEntityName = scriptArguments.dcsInputValidator.getRequestEntityFromXformObject("personalDatas", scriptArguments.xformInsertionMap["personalDatas"])
          if (!scriptArguments.totalEntityCounts[scriptArguments.requestEntityName]) {
            scriptArguments.totalEntityCounts[scriptArguments.requestEntityName] = 0
          }
          scriptArguments.totalEntityCounts[scriptArguments.requestEntityName]++

          if(scriptArguments.dcsDebugEnabled) {
          	scriptArguments.log.info("Acquire Xform at line 19 completed");
              addInfoDebugLog( debugParams, OperationType.XFORM, "Acquire Xform  completed" );
          }
          // end of XFORM statement at line 19 and col 4
    }
      if(scriptArguments.dcsDebugEnabled) {
          scriptArguments.log.info("Ifelse method at line 8 completed");
          addInfoDebugLog( debugParams, OperationType.IF, "Ifelse statement completed" );
      }
  }

  
import static Constants.*;
import java.util.regex.Matcher;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.XformConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.Pagination;
import com.intuit.platform.fdp.connectivitydsl.runtime.AbstractPaginationImpl;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.AcquireData;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.PaginationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.EntityIdentificationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.DcsInputValidator;
import com.intuit.platform.fdp.connectivitydsl.runtime.service.DcsInputService;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.DcsInputServiceNewI;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.AdditionalParameters;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.AcquireData;
import com.intuit.platform.fdp.connectivitydsl.runtime.Chunking;
import com.intuit.platform.fdp.connectivitydsl.runtime.newservice.ConnectivityServiceNewI;
import com.intuit.platform.fdp.connectivitydsl.runtime.AcquireSupport;
import com.intuit.platform.fdp.connectivitydsl.runtime.AbstractChunkingImpl;
import com.intuit.platform.dataacquisition.acquire.common.ApiLevelErrorV2;
import com.intuit.platform.dataacquisition.acquire.common.AccountLevelStatusV2;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.EntityIdentificationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.XformConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ChunkingConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.PaginationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.CustomPaginationConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.AbstractErrorHandler;
import com.intuit.platform.fdp.connectivitydsl.runtime.AbstractInjectedCodeRetryHandler;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.CustomErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ErrorCodeErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.HttpStatusErrorHandlerConfig;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ErrorCodeDefinition;
import com.intuit.platform.fdp.connectivitydsl.runtime.domain.ERROR_HANDLER_TYPE;
import com.intuit.platform.fdp.connectivitydsl.runtime.ErrorHandler;
//import com.intuit.platform.fdp.connectivitydsl.runtime.InjectedErrorHandlerCode;

import com.intuit.platform.dataacquisition.acquire.common.exception.ConnectivityExceptionV2;
import com.intuit.platform.dataacquisition.acquire.common.GenericAcquireRequest;
import com.intuit.platform.dataacquisition.acquire.common.reqsigner.RequestSignerFactory;
import com.intuit.platform.dataacquisition.acquire.common.reqsigner.RequestSigner;
import com.intuit.platform.fdp.connectivitydsl.runtime.DcsModifierDelegator;
import org.apache.commons.lang3.StringUtils;
import com.intuit.platform.dataacquisition.acquire.common.ApiLevelErrorV2;
import com.intuit.platform.fdp.connectivity.oauth.exception.OauthException;
import com.intuit.platform.fdp.connectivity.oauth.common.OauthError;
import com.intuit.platform.dataacquisition.acquire.common.ConfigurationProperties;
//import com.intuit.platform.fdp.connectivity.oauth.api.ConfigurationProperties;
import com.intuit.platform.fdp.connectivity.senddata.exception.ConnectAndTransformException;
import com.intuit.platform.fdp.connectivity.senddata.common.ConnectAndTransformError;
import com.intuit.platform.fdp.connectivitydsl.runtime.FlowMapper;


class Constants {
  static final DMESG = "%s: Failed to assign %s since %s is empty or null in %s at line %d\n";

  // Constants for callModifier injected code callModifierImplicitArgs map
  static final LOGGER = "logger"
  static final INPUT = "input"
  static final PARAMS = "params"
  static final MODIFIERNAME = "modifierName"
  static final ATTRIBUTENAME = "attributeName"
  static final PERFLOGMAPS = "perfLogMaps"
  static final ACCOUNTSTATUSBYACCOUNTMAP = "accountStatusByAccountMap"
  static final TOTALENTITYCOUNTS = "totalEntityCounts"
  static final TRANSACTIONCOUNTBYACCOUNTMAP = "transactionCountByAccountMap"
  static final INTEGER_OVERRIDE_TOTAL_ACCOUNTS_COUNT_INSERTED = "totalAccountsCountInserted"
  static final INTEGER_OVERRIDE_TOTAL_TRANSACTION_COUNT = "totalTransactionCount"
  static final PARSED_RESPONSE = "parsedResponse"
  static final HTTP_STATUS = "httpStatus"
  static final BOOLEAN_OVERRIDE_RESPONSE_BODY = "booleanOverrideResponseBody"

  static final ORIGINAL_ACQUIRE_DATA = "originalAcquireData"
  static final TEMPLATE_ACQUIRE_DATA = "templateAcquireData"
  static final PARSED_PREVIOUS_PAGE_RESPONSE = "parsedPreviousPageResponse"
  static final CURRENT_INDEX = "currentIndex"
  static final READONLY_CURRENT_COUNTER = "readOnlyCurrentCounter"
  static final READONLY_MAX_RETRY = "readOnlyMaxRetry"
  static final READONLY_RETRY_INTERVAL = "readOnlyRetryInterval"
  static final ENTITY_NAME      = "entityName"
  static final GENERIC_REQUEST  = "genericRequest"
  static final GENERIC_RESPONSE = "genericResponse"
  static final ROOT_ENTITY      = "rootEntity"

  //For request signer logging statements
  static String BUILDING_REQ_SIGNER_USING_ACQUIRE_COMMON = "Building Request signer using acquire common!!"
  static String BUILDING_CUSTOM_REQUEST_SIGNER = "Building Custom Request Signer !!"
  static String GETTING_INSTANCE_OF_REQ_SIGNER = "Getting instance of request signer: {}"
  static String FAILBACK_REQ_SIGNER_BUILD_MSG  = "CustomReqSigner is null. Attempting building req signer from library."
  static String SIGNING_KEY_AND_EMPTY_VALUE_MSG = "signingKey: {} has empty value: {}"
  static String WSI_REQ_SIGNER_KEY             = "fdp.wsi.request.signer"
  static String WSI_REQ_SIGNER_BASE_KEY        = "fdp.wsi.request.signer.key"
  static String INJECTED_CODE_VARS = "injectedCodeVars"

  // user defined functions
  static DcsModifierDelegator dcsModifierDelegator = initializeDcsModifierDelegator();

  private static initializeDcsModifierDelegator() {
    DcsModifierDelegator iDcsModifierDelegator = new DcsModifierDelegator();
    iDcsModifierDelegator.addModifier("avatarExcludes", ["header.x-forwarded-port": "header.x-forwarded-port", "header.x-oil-channel-id": "header.x-oil-channel-id", "header.x-transactiontype": "header.x-transactiontype", "header.x-amex-api-key": "header.x-amex-api-key", "header.x-forwarded-proto": "header.x-forwarded-proto", "header.x-amex-request-id": "header.x-amex-request-id", "header.x-rootspanid": "header.x-rootspanid", "header.intuit_tid": "header.intuit_tid", "header.content-type": "header.content-type", "header.x-oil-realm-id": "header.x-oil-realm-id", "header.x-amzn-trace-id": "header.x-amzn-trace-id", "header.singularityheader": "header.singularityheader", "header.accept-encoding": "header.accept-encoding", "header.partneruserid": "header.partneruserid", "header.x-parentspanid": "header.x-parentspanid", "header.cache-control": "header.cache-control", "header.user-agent": "header.user-agent", "header.host": "header.host", "header.x-oil-credential-set-id": "header.x-oil-credential-set-id", "header.x-oil-provider-id": "header.x-oil-provider-id", "header.x-forwarded-for": "header.x-forwarded-for", "header.x-real-ip": "header.x-real-ip", "header.accept": "header.accept", "header.x-oil-fdp-id": "header.x-oil-fdp-id", "header.x-oil-user-id": "header.x-oil-user-id"], null)
    iDcsModifierDelegator.addModifier("account_status_map", ["active": "OPEN"], null)
    iDcsModifierDelegator.addModifier("account_type_map", ["active": "OPEN"], null)
    iDcsModifierDelegator.addModifier("loc_type_map", ["companycard": "CREDITCARD", "corporatecard": "CREDITCARD", "consumercard": "CREDITCARD"], null)
    return iDcsModifierDelegator;
  }
}

class InjectedCode {
  static def definedInjectedModifiers = intializeDefinedInjectedModifiers()


  private static intializeDefinedInjectedModifiers() {

    def iDefinedInjectedModifiers = [] as Set
    return iDefinedInjectedModifiers
  }

  static def  callModifier(def callModifierImplicitArgs, String modifierName, String attributeName, def input, Object... params) {
    Logger log = callModifierImplicitArgs[LOGGER];
    callModifierImplicitArgs[MODIFIERNAME] = modifierName;
    callModifierImplicitArgs[ATTRIBUTENAME] = attributeName;
    callModifierImplicitArgs[INPUT] = input;
    callModifierImplicitArgs[PARAMS] = params;
    try {

      if (!definedInjectedModifiers.contains(modifierName)) {
        return dcsModifierDelegator(modifierName, attributeName, input, params)
      }

      // Invoke custom injected code by reflection
      return "$modifierName"(callModifierImplicitArgs)
    } catch (Exception e) {
      log.error(
        "Error while invoking custom injected modifier: '{}' for attribute: '{}' with exception: {}",
        modifierName, attributeName, e
      );
      return null;
    }
  }

  static def callRetrieveCustomSchemaValidationRules(def customSchemaValidationImplicitArgs, Logger log, String modifierName,
                                        def input, String entityType, def genericRequest, def genericResponse, def rootEntity, Object... params){
      if (!definedInjectedModifiers.contains(modifierName)) {
            log.error("Missing custom schemavalidationsrules method: "+modifierName);
            ApiLevelErrorV2 apiLevelErrorV2 = ApiLevelErrorV2.fromName("general-101");
            throw new ConnectivityExceptionV2(apiLevelErrorV2, null, "Missing custom schemavalidationsrules method: "+modifierName);
      }
      customSchemaValidationImplicitArgs[LOGGER] = log
      customSchemaValidationImplicitArgs[INPUT] = input
      customSchemaValidationImplicitArgs[PARAMS] = params
      customSchemaValidationImplicitArgs[ENTITY_NAME] = entityType
      customSchemaValidationImplicitArgs[GENERIC_REQUEST] = genericRequest
      customSchemaValidationImplicitArgs[GENERIC_RESPONSE] = genericResponse
      customSchemaValidationImplicitArgs[ROOT_ENTITY] = rootEntity

      return "$modifierName"(customSchemaValidationImplicitArgs);
  }

  static def callCustomErrorHandler(def customErrorHandlingImplicitArgs, Logger log, String modifierName, def input, def httpStatus, Object... params) {
      if (!definedInjectedModifiers.contains(modifierName)) {
        return null;
      }

      boolean stopErrorHandling = false;
      customErrorHandlingImplicitArgs[LOGGER] = log
      customErrorHandlingImplicitArgs[INPUT] = input
      customErrorHandlingImplicitArgs[PARAMS] = params
      customErrorHandlingImplicitArgs[HTTP_STATUS] = httpStatus

      // Invoke custom injected code by reflection
      String customErrorHandlingStatus = "$modifierName"(customErrorHandlingImplicitArgs)
      if( customErrorHandlingStatus != null && customErrorHandlingStatus.equals("STOP_ERROR_HANDLING") ){
        stopErrorHandling = true
      }

      return stopErrorHandling;
  }

   // static def callCustomAssertHandler(def customAssertHandlingImplicitArgs, Logger log, String modifierName, def input, Object... params) {
   //     if (!definedInjectedModifiers.contains(modifierName)) {
   //       return null;
   //     }
   //     customAssertHandlingImplicitArgs[LOGGER] = log
   //     customAssertHandlingImplicitArgs[INPUT] = input
   //     customAssertHandlingImplicitArgs[PARAMS] = params
   //
   //     // Invoke custom injected code by reflection
   //     return "$modifierName"(customAssertHandlingImplicitArgs)
   // }

  static def callCustomPostProcessor(def postProcessImplicitArgs, Object... params) {
    String modifierName = postProcessImplicitArgs["postProcessorName"];
    if (modifierName == null) {
      return null;
    } else if (!definedInjectedModifiers.contains(modifierName)) {
      return null;
    }

    boolean stopPostProcessing = false;

    // Invoke custom injected code using reflection
    String postProcessingStatus;
    try {
      postProcessingStatus = "$modifierName"(postProcessImplicitArgs, params);
    } catch (ConnectivityExceptionV2 connectivityException) {
      throw connectivityException;
    } catch (OauthException oauthException) {
      throw oauthException;
    } catch (Exception e) {
      // Create generic ConnectivityExceptionV2 and throw that
      ApiLevelErrorV2 apiLevelErrorV2 = ApiLevelErrorV2.fromName("general-101");
      throw new ConnectivityExceptionV2(apiLevelErrorV2, null, "No ConnectivityExceptionV2 or OauthException error thrown by user from postprocess clause: " + e);
    }

    if (postProcessingStatus != null && postProcessingStatus.equals("STOP_POST_PROCESSING")) {
      stopPostProcessing = true;
    }

    return stopPostProcessing;
  }

  static def callCustomRequestSignerBuilder(Logger log, GenericAcquireRequest genericAcquireRequest) {
      if (!definedInjectedModifiers.contains("customRequestSigner")) {
        log.debug(BUILDING_REQ_SIGNER_USING_ACQUIRE_COMMON)
        return requestSignerFromLibrary(log, genericAcquireRequest);
      }

      log.debug(BUILDING_CUSTOM_REQUEST_SIGNER)

      RequestSigner reqSigner = customRequestSigner(log, genericAcquireRequest);
      if(reqSigner == null){
        log.info(FAILBACK_REQ_SIGNER_BUILD_MSG);
        return requestSignerFromLibrary(log, genericAcquireRequest);
      }

      return reqSigner;
  }


   static def callXform_dynamic(Logger log,def fnName,def pe, XformConfig xformConfig, def perfLogMaps, def currentIndex, def parentJson, def dynamicMappingConfig) {
           if (!definedInjectedModifiers.contains("xform_dynamic")) {
               log.error("Error Calling Xform_dynamic. No custom Method Defined.");
               return parentJson;
             }
       return xform_dynamic(fnName,pe,xformConfig,perfLogMaps,currentIndex,parentJson,dynamicMappingConfig);
    }

  static def requestSignerFromLibrary(Logger log, GenericAcquireRequest acquireRequest) throws ConnectivityExceptionV2{    
    if(acquireRequest.getConfigurationProperties() != null 
        && acquireRequest.getConfigurationProperties().getProviderConfiguration() != null ){
        Map<String, String> providerConfig = acquireRequest.getConfigurationProperties().getProviderConfiguration();
        String wsiReqSignerKeyValue = providerConfig.get(WSI_REQ_SIGNER_KEY)
        if( StringUtils.isNotEmpty(wsiReqSignerKeyValue) ){
            log.info(GETTING_INSTANCE_OF_REQ_SIGNER, wsiReqSignerKeyValue);
            return RequestSignerFactory.getInstance(log, wsiReqSignerKeyValue, populateSigningValues(log, providerConfig))
        }
        return null;
    }
    return null;
  }

  /* This method will take a provider config object passed by wsi and construct an array of signing keys */
  static String[] populateSigningValues(Logger log, Map<String, String> providerConfig) {
      int keyCounter=1;
      if(providerConfig != null && StringUtils.isNotEmpty(providerConfig.get(WSI_REQ_SIGNER_KEY)) ){
          String signingKey=providerConfig.get(WSI_REQ_SIGNER_BASE_KEY+keyCounter);
          List<String> signingKeyValues = new ArrayList<>();
          while(StringUtils.isNotEmpty(signingKey)) {
              String signingKeyValue = null;
              signingKeyValue = signingKey.startsWith(WSI_REQ_SIGNER_KEY)? providerConfig.get(signingKey): signingKey;
              log.debug(SIGNING_KEY_AND_EMPTY_VALUE_MSG,signingKey, StringUtils.isEmpty(signingKeyValue))
              signingKeyValues.add(signingKeyValue);
              keyCounter++;
              signingKey=providerConfig.get(WSI_REQ_SIGNER_BASE_KEY+keyCounter);
          }
          return signingKeyValues.toArray(new String[signingKeyValues.size()]);
      }

      return null;
  }

}

class InjectedPaginationCode extends AbstractPaginationImpl {

    CustomPaginationConfig customPaginationConfig;
    public InjectedPaginationCode(CustomPaginationConfig customPaginationConfig){
        this.customPaginationConfig = customPaginationConfig;
    }

// User will specify in the spec which methods to execute first based off of the Map, injectedPaginationCodeImplicitArgs
  AcquireData getAcquireDataCustomPaginationTemplate(AcquireData originalAcquireData){
    def listOfParams =customPaginationConfig.getPagingParamsList()
    String customPagingMethodName0 = listOfParams[0]
    Logger logger = customPaginationConfig.getLog()
    String modifierFromDsl = customPaginationConfig.getModifierName()

    Map<String,Object> implicitArgsMap = new HashMap<>();
    implicitArgsMap[LOGGER] = logger;
    implicitArgsMap[PARAMS] = listOfParams;
    implicitArgsMap[ORIGINAL_ACQUIRE_DATA] = originalAcquireData;
    implicitArgsMap[MODIFIERNAME] = modifierFromDsl;

    return "$customPagingMethodName0"(implicitArgsMap)
  }

  public void modifyFirstAcquireDataRequestWithCustomPaginationTemplate(AcquireData acquireData, AcquireData template){
    def listOfParams =customPaginationConfig.getPagingParamsList()
    String customPagingMethodName1 = listOfParams[1]
    Logger logger = customPaginationConfig.getLog()
    String modifierFromDsl = customPaginationConfig.getModifierName()

    //String modifierName = listOfParams[1]
    Map<String,Object> implicitArgsMap = new HashMap<>();
    implicitArgsMap[LOGGER] = logger;
    implicitArgsMap[PARAMS] = listOfParams;
    implicitArgsMap[ORIGINAL_ACQUIRE_DATA] = acquireData;
    implicitArgsMap[TEMPLATE_ACQUIRE_DATA] = template;
    implicitArgsMap[MODIFIERNAME] = modifierFromDsl;
    logger.debug("Before calling method: {}", "$customPagingMethodName1")

    "$customPagingMethodName1"(implicitArgsMap)
  }

  AcquireData getNextAcquireDataFromPaginationTemplate(AcquireData acquireData, Object parsedResponse, int currIndex){

    def listOfParams =customPaginationConfig.getPagingParamsList()
    String customPagingMethodName2 = listOfParams[2]
    Logger logger = customPaginationConfig.getLog()
    String modifierFromDsl = customPaginationConfig.getModifierName()

    //String modifierName = listOfParams[1]
    Map<String,Object> implicitArgsMap = new HashMap<>();
    implicitArgsMap[LOGGER] = logger;
    implicitArgsMap[PARAMS] = listOfParams;
    implicitArgsMap[MODIFIERNAME] = modifierFromDsl;
    implicitArgsMap[TEMPLATE_ACQUIRE_DATA] = acquireData;
    implicitArgsMap[PARSED_PREVIOUS_PAGE_RESPONSE] = parsedResponse
    implicitArgsMap[CURRENT_INDEX] = currIndex;

    "$customPagingMethodName2"(implicitArgsMap)

  }

}

class InjectedCodeChunking extends AbstractChunkingImpl {

  static String LOGGER = "log"
  static String CHUNKING_PARAMS_LIST = "chunkingParamsList"
  static String HTTP_CLIENT = "httpClient"
  static String ACQUIRE_DATA = "acquireData"
  static String PAGINATION_CONFIG = "paginationConfig"
  static String ENTITY_IDENTIFICATION_CONFIG = "entityIdentificationConfig"

  ChunkingConfig chunkingConfig = null;

  public InjectedCodeChunking(ChunkingConfig chunkingConfig){
    this.chunkingConfig = chunkingConfig;
  }

  public List<AcquireData> getMultipleAcquireCallsForChunking(AcquireData acquireData, 
                                                                PaginationConfig paginationConfig, 
                                                                EntityIdentificationConfig entityIdentificationConfig,
                                                                AdditionalParameters additionalParameters) {
    List chunkingParamsList = new ArrayList();
    Logger log = null;
    Map<String,Object> implicitArgsMap = new HashMap<>();
    implicitArgsMap[InjectedCodeChunking.LOGGER] = chunkingConfig.getLog();
    implicitArgsMap[InjectedCodeChunking.CHUNKING_PARAMS_LIST] = chunkingConfig.getChunkingParamsList();
    implicitArgsMap[InjectedCodeChunking.ACQUIRE_DATA] = acquireData;
    implicitArgsMap[InjectedCodeChunking.PAGINATION_CONFIG] = paginationConfig;
    implicitArgsMap[InjectedCodeChunking.ENTITY_IDENTIFICATION_CONFIG] = entityIdentificationConfig;
    String chunkingModifierName = chunkingConfig.getChunkingModifierName();
    return "$chunkingModifierName"(implicitArgsMap);
  }


}

class InjectedCodeRetryHandler extends AbstractInjectedCodeRetryHandler{

    static def definedInjectedModifiers = intializeDefinedInjectedModifiers()

    private static intializeDefinedInjectedModifiers() {

        def iDefinedInjectedModifiers = [] as Set
        return iDefinedInjectedModifiers
    }

    public InjectedCodeRetryHandler(Map<String, Integer> retryParams,  Map customRetryHandlingImplicitArgs){
        super(retryParams, customRetryHandlingImplicitArgs)
    }

    @Override
    public boolean isRetryNeeded(Map customRetryHandlingImplicitArgs, Object parsedResponse, AcquireData acquireData) {
       if (!definedInjectedModifiers.contains(this.retryParams.handlerName)) {
           return false;
       }
       customRetryHandlingImplicitArgs[HTTP_STATUS] = acquireData.getHttpStatusCode();
       customRetryHandlingImplicitArgs[ORIGINAL_ACQUIRE_DATA] = acquireData;
       customRetryHandlingImplicitArgs[PARSED_RESPONSE] = parsedResponse;
       customRetryHandlingImplicitArgs[READONLY_CURRENT_COUNTER] = this.retryParams.get("currentCounter");
       customRetryHandlingImplicitArgs[READONLY_MAX_RETRY] = this.retryParams.get("maxTimes");
       customRetryHandlingImplicitArgs[READONLY_RETRY_INTERVAL] = this.retryParams.get("interval");

       // Invoke custom injected code by reflection;
       return callModifier(this.retryParams.handlerName, customRetryHandlingImplicitArgs);
   }

   static def callModifier(String modifier, Map customRetryHandlingImplicitArgs){
        return InjectedCode."$modifier"(customRetryHandlingImplicitArgs);
   }
}

class InjectedErrorHandlerCode extends AbstractErrorHandler{

    static def definedInjectedModifiers = intializeDefinedInjectedModifiers()

    private static intializeDefinedInjectedModifiers() {

        def iDefinedInjectedModifiers = [] as Set
        return iDefinedInjectedModifiers
    }


    List<ErrorHandlerConfig> errorHandlerConfigList ;
    AcquireSupport acquireSupport;
    ConnectivityServiceNewI connectivityService;
    boolean on200HttpStatusErrorHandler = false;
    boolean onOkHttpStatusErrorHandler = false;
    def customErrorHandlingImplicitArgs = [:]

    public InjectedErrorHandlerCode(List<ErrorHandlerConfig> errorHandlerConfigList, AcquireSupport acquireSupport, def customErrorHandlingImplicitArgs){

        this.errorHandlerConfigList = errorHandlerConfigList;
        this.acquireSupport = acquireSupport;
        this.customErrorHandlingImplicitArgs = customErrorHandlingImplicitArgs
        for (ErrorHandlerConfig errorHandlerConfig:this.errorHandlerConfigList){

            if(!on200HttpStatusErrorHandler)
                on200HttpStatusErrorHandler = !on200HttpStatusErrorHandler && errorHandlerConfig.isEnableHandlerOn200HttpStatus();

            if(!onOkHttpStatusErrorHandler)
                onOkHttpStatusErrorHandler = !onOkHttpStatusErrorHandler &&  errorHandlerConfig.isEnableHandlerOnOkHttpStatus();

            if(on200HttpStatusErrorHandler && onOkHttpStatusErrorHandler)
                break;
        }
    }

    public InjectedErrorHandlerCode(List<ErrorHandlerConfig> errorHandlerConfigList, ConnectivityServiceNewI connectivityService, def customErrorHandlingImplicitArgs){

        this.errorHandlerConfigList = errorHandlerConfigList;
        this.connectivityService = connectivityService;
        this.customErrorHandlingImplicitArgs = customErrorHandlingImplicitArgs
        for (ErrorHandlerConfig errorHandlerConfig:this.errorHandlerConfigList){

            if(!on200HttpStatusErrorHandler)
                on200HttpStatusErrorHandler = !on200HttpStatusErrorHandler && errorHandlerConfig.isEnableHandlerOn200HttpStatus();

            if(!onOkHttpStatusErrorHandler)
                onOkHttpStatusErrorHandler = !onOkHttpStatusErrorHandler &&  errorHandlerConfig.isEnableHandlerOnOkHttpStatus();

            if(on200HttpStatusErrorHandler && onOkHttpStatusErrorHandler)
                break;
        }
    }

    public InjectedErrorHandlerCode(List<ErrorHandlerConfig> errorHandlerConfigList, ConnectivityServiceNewI connectivityService, def customErrorHandlingImplicitArgs ,FlowMapper flowMapper){

          this.errorHandlerConfigList = errorHandlerConfigList;
          this.connectivityService = connectivityService;
          this.customErrorHandlingImplicitArgs = customErrorHandlingImplicitArgs
          this.setFlowMapper(flowMapper);
          for (ErrorHandlerConfig errorHandlerConfig:this.errorHandlerConfigList){

              if(!on200HttpStatusErrorHandler)
                  on200HttpStatusErrorHandler = !on200HttpStatusErrorHandler && errorHandlerConfig.isEnableHandlerOn200HttpStatus();

              if(!onOkHttpStatusErrorHandler)
                  onOkHttpStatusErrorHandler = !onOkHttpStatusErrorHandler &&  errorHandlerConfig.isEnableHandlerOnOkHttpStatus();

              if(on200HttpStatusErrorHandler && onOkHttpStatusErrorHandler)
                  break;
          }
      }

  public boolean isOkHttpStatusErrorHandlingEnabled() {
        return this.onOkHttpStatusErrorHandler;
    }

    public boolean is200HttpStatusErrorHandlingEnabled() {
        return this.on200HttpStatusErrorHandler;
    }

    public void evalauateErrorConditions(AcquireData acquireData, Object parsedResponse) {
        if(acquireSupport != null){
            evalauateErrorConditionsLegacy(acquireData, parsedResponse);
        }
        else{
            evalauateErrorConditionsNew(acquireData, parsedResponse);
        }
    }

    void evalauateErrorConditionsLegacy(AcquireData acquireData, Object parsedResponse) {
        boolean stopErrorHandling = false;
        ConnectivityExceptionV2 connectivityExceptionV2 = null;

        Object errorValue = null;
        boolean errorValueFound = false;

        for (ErrorHandlerConfig errorHandlerConfig : errorHandlerConfigList) {
            String errorMapName = errorHandlerConfig.getErrorMapName();
            Logger log = errorHandlerConfig.getLogger();

            if( !acquireSupport.isOkHttpStatus(acquireData.getHttpStatusCode()) ||
                    ( ( acquireSupport.isOkHttpStatus(acquireData.getHttpStatusCode()) && errorHandlerConfig.isEnableHandlerOnOkHttpStatus() )
                            || (acquireData.getHttpStatusCode().toString().equals("200") && errorHandlerConfig.isEnableHandlerOn200HttpStatus() )
                    )
            ) {

                if (errorHandlerConfig.isCustomErrorHandler()) {
                    try {
                        log.debug("entered custom error handler!!");
                        CustomErrorHandlerConfig customErrorHandlerConfig = (CustomErrorHandlerConfig) errorHandlerConfig;
                        stopErrorHandling = callCustomErrorHandler(this.customErrorHandlingImplicitArgs, log, errorMapName
                                , (customErrorHandlerConfig.getVariable()== null ? parsedResponse: customErrorHandlerConfig.getVariable()),  acquireData.getHttpStatusCode().toString(), customErrorHandlerConfig.getModifierParamsGroovyVariableList().toArray());

                    } catch (ConnectivityExceptionV2 connectivityException) {
                        log.error("error occured {}", connectivityException);
                        connectivityExceptionV2 = connectivityException;
                    }
                } else {


                    if (errorHandlerConfig.isHttpStatusErrorHandler()) {
                        log.debug("entered http status error handler!!");
                        errorValue = acquireData.getHttpStatusCode().toString();
                    } else {
                        log.debug("entered error code error handler!!");
                        errorValue = null;
                        errorValueFound = false;
                        ErrorCodeErrorHandlerConfig errorCodeErrorHandlerConfig = (ErrorCodeErrorHandlerConfig) errorHandlerConfig;
                        List<ErrorCodeDefinition> errorCodeDefinitionList = errorCodeErrorHandlerConfig.getErrorCodeDefinitionList();
                        for (ErrorCodeDefinition errorCodeDefinition : errorCodeDefinitionList) {

                            def errorVariable = errorCodeDefinition.getVariable();
                            if(errorVariable == null){
                                int indexOfBaseVar = errorCodeDefinition.getBaseAttribute().indexOf("[")

                                if( indexOfBaseVar == -1)
                                    errorVariable = parsedResponse;
                                else{
                                    if(parsedResponse instanceof List) {
                                        indexOfBaseVar   = Integer.parseInt(errorCodeDefinition.getBaseAttribute().substring(indexOfBaseVar+1, errorCodeDefinition.getBaseAttribute().length()-1));
                                        errorVariable = parsedResponse[indexOfBaseVar];
                                    }
                                    else{
                                        continue;
                                    }
                                }
                            }

                            errorValue = errorValue == null ?
                                    retrieveErrorValue(errorCodeErrorHandlerConfig.getLogger(),
                                            errorCodeErrorHandlerConfig.getProviderResponseValidator(),
                                            errorVariable,
                                            errorCodeDefinition.getBaseAttribute(),
                                            errorCodeDefinition.getAttributesSubStrList().toArray(new String[errorCodeDefinition.getAttributesSubStrList().size()])) :
                                    errorValue;

                        }
                    }

                    connectivityExceptionV2 = getErrorMappedConnectivityExceptionV2(errorMapName, errorValue, log);

                }

                if (connectivityExceptionV2 != null) {
                    log.debug("connectivityExceptionV2 obtained!!");
                    throw connectivityExceptionV2;
                }

                if(stopErrorHandling){
                    break;
                }
            }

        }

        if( !stopErrorHandling && !acquireSupport.isOkHttpStatus(acquireData.getHttpStatusCode()) ){
            log.debug("throwing generic http status exception!!");
            throw acquireSupport.getGenericBadHttpStatusException(acquireData);
        }
    }

    void evalauateErrorConditionsNew(AcquireData acquireData, Object parsedResponse) {
        boolean stopErrorHandling = false;
        ConnectivityExceptionV2 connectivityExceptionV2 = null;
        ConnectAndTransformException connectAndTransformException =null

        OauthException oauthException = null
        Object errorValue = null;
        boolean errorValueFound = false;

        for (ErrorHandlerConfig errorHandlerConfig : errorHandlerConfigList) {
            String errorMapName = errorHandlerConfig.getErrorMapName();
            Logger log = errorHandlerConfig.getLogger();

            if( !connectivityService.isOkHttpStatus(acquireData.getHttpStatusCode()) ||
                    ( ( connectivityService.isOkHttpStatus(acquireData.getHttpStatusCode()) && errorHandlerConfig.isEnableHandlerOnOkHttpStatus() )
                            || (acquireData.getHttpStatusCode().toString().equals("200") && errorHandlerConfig.isEnableHandlerOn200HttpStatus() )
                    )
            ) {

                if (errorHandlerConfig.isCustomErrorHandler()) {
                    try {
                        log.debug("entered custom error handler!!");
                        CustomErrorHandlerConfig customErrorHandlerConfig = (CustomErrorHandlerConfig) errorHandlerConfig;
                        stopErrorHandling = callCustomErrorHandler(this.customErrorHandlingImplicitArgs, log, errorMapName
                                , (customErrorHandlerConfig.getVariable()== null ? parsedResponse: customErrorHandlerConfig.getVariable()),  acquireData.getHttpStatusCode().toString(), customErrorHandlerConfig.getModifierParamsGroovyVariableList().toArray());

                    } catch (ConnectivityExceptionV2 connectivityException) {
                        log.error("error occured {}", connectivityException);
                        connectivityExceptionV2 = connectivityException;
                    }
                    catch (ConnectAndTransformException exception){
                         log.error("error occured {}", exception);
                         connectAndTransformException = exception;

                    } catch (OauthException exception) {
                         log.error("error occured {}", exception);
                         oauthException = exception;
                         System.out.println("catch oauthException");

                    }
                } else {
                    if (errorHandlerConfig.isHttpStatusErrorHandler()) {
                        log.debug("entered http status error handler!!");
                        errorValue = acquireData.getHttpStatusCode().toString();
                    } else {
                        log.debug("entered error code error handler!!");
                        errorValue = null;
                        errorValueFound = false;
                        ErrorCodeErrorHandlerConfig errorCodeErrorHandlerConfig = (ErrorCodeErrorHandlerConfig) errorHandlerConfig;
                        List<ErrorCodeDefinition> errorCodeDefinitionList = errorCodeErrorHandlerConfig.getErrorCodeDefinitionList();
                        for (ErrorCodeDefinition errorCodeDefinition : errorCodeDefinitionList) {

                            def errorVariable = errorCodeDefinition.getVariable();
                            if(errorVariable == null){
                                int indexOfBaseVar = errorCodeDefinition.getBaseAttribute().indexOf("[")

                                if( indexOfBaseVar == -1)
                                    errorVariable = parsedResponse;
                                else{
                                    if(parsedResponse instanceof List) {
                                        indexOfBaseVar   = Integer.parseInt(errorCodeDefinition.getBaseAttribute().substring(indexOfBaseVar+1, errorCodeDefinition.getBaseAttribute().length()-1));
                                        errorVariable = parsedResponse[indexOfBaseVar];
                                    }
                                    else{
                                        continue;
                                    }
                                }
                            }

                            errorValue = errorValue == null ?
                                    retrieveErrorValue(errorCodeErrorHandlerConfig.getLogger(),
                                            errorCodeErrorHandlerConfig.getProviderResponseValidator(),
                                            errorVariable,
                                            errorCodeDefinition.getBaseAttribute(),
                                            errorCodeDefinition.getAttributesSubStrList().toArray(new String[errorCodeDefinition.getAttributesSubStrList().size()])) :
                                    errorValue;

                        }
                    }
                    boolean isConnectAndTransformFlow = false;
                    boolean isOauthFlow = false;

                    if(null != this.flowMapper) {
                        isConnectAndTransformFlow = this.flowMapper.isConnectAndTransformFlow();
                        isOauthFlow = this.flowMapper.isOauthFlow();
                    }
                    if(isConnectAndTransformFlow){
                        connectAndTransformException  = getErrorMappedConnectAndTransformException(errorMapName, errorValue, log);
                    } else if(isOauthFlow) {
                        oauthException = getErrorMappedOauthException(errorMapName, errorValue, log);
                    } else {
                        connectivityExceptionV2 = getErrorMappedConnectivityExceptionV2(errorMapName, errorValue, log);

                    }

                }
                  if (oauthException != null) {
                        log.debug("oauthException obtained!!");
                        throw oauthException;
                    }
                if (connectivityExceptionV2 != null) {
                    log.debug("connectivityExceptionV2 obtained!!");
                    throw connectivityExceptionV2;
                }

                  if (connectAndTransformException != null) {
                        log.debug("connectAndTransformException obtained!!");
                        throw connectAndTransformException;
                    }


                if(stopErrorHandling){
                    break;
                }
            }

        }

        if( !stopErrorHandling && !connectivityService.isOkHttpStatus(acquireData.getHttpStatusCode()) ){
            log.debug("throwing generic http status exception!!");
            throw connectivityService.getGenericBadHttpStatusException(acquireData);
        }
    }

    // TODO: WSI-703: this needs to be generalized for all children entities
    // for now try to get entity level mapping first
    // if that fails, then try to get api level mapping
    private ConnectivityExceptionV2 getErrorMappedConnectivityExceptionV2(String errorMapName, def errorValue, def log) {
        if (errorMapName != null && errorValue != null) {
            def firstErrorValue = (errorValue instanceof List) ? errorValue[0] : errorValue;
            log.debug("Mapping error code {} using {}", firstErrorValue, errorMapName);

            if (firstErrorValue != null) {
                PerfThreadContext.get().addField("error_code_host", String.valueOf(firstErrorValue));
                def fdpErrorCode = dcsModifierDelegator.call(errorMapName, "handleerror", firstErrorValue)
                log.info("mapped error code from {} to {}", firstErrorValue, fdpErrorCode);
                if (fdpErrorCode == null) {
                    return null;
                }
                AccountLevelStatusV2 accountLevelStatusV2 = AccountLevelStatusV2.fromName(fdpErrorCode);
                log.debug("accountLevelStatusV2 {}", accountLevelStatusV2);
                if (accountLevelStatusV2 != null) {
                    return new ConnectivityExceptionV2(accountLevelStatusV2);
                }

                ApiLevelErrorV2 apiLevelErrorV2 = ApiLevelErrorV2.fromName(fdpErrorCode);
                if (apiLevelErrorV2 != null) {
                    return new ConnectivityExceptionV2(apiLevelErrorV2, null, "Bad response from provider");
                }
            }
        }
        return null;
    }




private ConnectAndTransformException getErrorMappedConnectAndTransformException(String errorMapName, def errorValue, def log) {
    if (errorMapName != null && errorValue != null) {
        def firstErrorValue = (errorValue instanceof List) ? errorValue[0] : errorValue;
        log.debug("Mapping error code {} using {}", firstErrorValue, errorMapName);

        if (firstErrorValue != null) {
            PerfThreadContext.get().addField("error_code_host", String.valueOf(firstErrorValue));
            def fdpErrorCode = dcsModifierDelegator.call(errorMapName, "handleerror", firstErrorValue)

            log.info("mapped error code from {} to {}", firstErrorValue, fdpErrorCode);
            if (fdpErrorCode == null) {
                return null;
            }

            ConnectAndTransformError connectAndTransformError = ConnectAndTransformError.fromCode(fdpErrorCode);

            if (connectAndTransformError != null) {
                return new ConnectAndTransformException(connectAndTransformError);
            }
        }
    }
    return null;
}

private OauthException getErrorMappedOauthException(String errorMapName, def errorValue, def log) {
    if (errorMapName != null && errorValue != null) {
        def firstErrorValue = (errorValue instanceof List) ? errorValue[0] : errorValue;
        log.debug("Mapping error code {} using {}", firstErrorValue, errorMapName);

        if (firstErrorValue != null) {
            PerfThreadContext.get().addField("error_code_host", String.valueOf(firstErrorValue));
            def fdpErrorCode = dcsModifierDelegator.call(errorMapName, "handleerror", firstErrorValue)

            log.info("mapped error code from {} to {}", firstErrorValue, fdpErrorCode);
            if (fdpErrorCode == null) {
                return null;
            }

            OauthError oauthError = OauthError.fromCode(fdpErrorCode);

            if (oauthError != null) {
                return new OauthException(oauthError);
            }
        }
    }
    return null;
}


    private def retrieveErrorValue(def log, def providerResponseValidator, def variable, String varName, String... path){
        def errorValue = null;
        try{
           errorValue = providerResponseValidator.getExprValue(variable, varName, path);
        } catch (ConnectivityExceptionV2 e) {
          log.error("Couldn't access the error field from provider response: {} - {}", variable, e.getMessage());
        } catch (OauthException e) {
                   log.error("Couldn't access the error field from provider response: {} - {}", variable, e.getMessage());
        }
        return errorValue;
    }

    static def callCustomErrorHandler(def customErrorHandlingImplicitArgs, Logger log, String modifierName, def input, def httpStatus, Object... params) {
        if (!definedInjectedModifiers.contains(modifierName)) {
            return null;
        }

        boolean stopErrorHandling = false;
        customErrorHandlingImplicitArgs[LOGGER] = log
        customErrorHandlingImplicitArgs[INPUT] = input
        customErrorHandlingImplicitArgs[PARAMS] = params
        customErrorHandlingImplicitArgs[HTTP_STATUS] = httpStatus
        // Invoke custom injected code by reflection
        String customErrorHandlingStatus = "$modifierName"(customErrorHandlingImplicitArgs)

        if( customErrorHandlingStatus != null && customErrorHandlingStatus.equals("STOP_ERROR_HANDLING") ){
            stopErrorHandling = true
        }

        return stopErrorHandling;
    }


}

private void assign(String providerId, Map root, List parents, String attrName,  Object val, String fn, int line, String pattrName, String rhs) {
  Logger log = LoggerFactory.getLogger("dcs." + providerId + ".agent.groovy");
  log.trace("Assigning attribute: {}", attrName);
  if (val != null) {
    def parent = root;
    parents.each {
      def pName = "${it}"
      def idx = null;
      Matcher m = "${it}" =~ /(?<arrName>.+)\[(?<index>.*)\]$/
      if (m.matches()) {
        pName = m.group("arrName");
        idx = m.group("index");       
      }
      def iobj = parent[pName];
      if (null == iobj) {
        if (null == idx) {
          iobj = new HashMap();
        } else {
          iobj = new ArrayList();
          iobj = array_fill_upto(iobj, idx)
        }        
        parent["${it}"] = iobj;
      } else {
        if (null != idx) {
          int idxn = idx;
          if (!iobj instanceof List) {
            // TODO: fix the following in test classpath
            //throw new IllegalTransformationException("Attribute container is not an array", fn, line, pAttrName);
            String exMsg = "Attribute container being assigned :" + pAttrName + " is not an array in file: " + fn + " at line: " + line;
            throw new IllegalArgumentException(exMsg);
          }
          iobj = array_fill_upto(iobj, idx);
        }
      }
      parent = iobj;
    }
    log.trace("Attaching value");
    // need the following since XmlSlurper deserializes into GPathResult that cannot be serialized in to JSON as-is
    // TODO: would be more performant, if the following code is conditionally compiled
    parent[attrName] = (val instanceof groovy.util.slurpersupport.GPathResult) ? val.text() : val;
  } else {
    def logmsg = sprintf(DMESG, fn, attrName, pattrName, rhs, line);
    log.debug(logmsg);
  }
}

private Map array_fill_upto(List arr, int upto) {
  def arrSize = arr.size();
  if (arrSize  < utpo + 1) {
    for (int ix = 0; ix < upto + 1 - arrSize; ix++) {
      arr.add(new HashMap());
    } 
  }
  return arr.get(upto);
}

public Map xform(String filename, def providerEntityMap, XformConfig rawXformConfig, def perfLogMaps,def dynamicMappingConfig) {
    XformConfig xformConfig = (rawXformConfig != null) ? rawXformConfig : new XformConfig();
    dcsModifierDelegator.setXformConfigData(xformConfig);
    switch(filename) {
        case "userguid.xform":
            return xformMethod1(providerEntityMap, xformConfig, perfLogMaps, dynamicMappingConfig);
    }
}






private Map xformMethod1(def pe, XformConfig xformConfig, def perfLogMaps,def dynamicMappingConfig) {
    String providerId = xformConfig.getProviderId();
    DcsInputValidator dcsInputValidator = xformConfig.getDcsInputValidator();
    DcsInputService dcsInputService = xformConfig.getDcsInputService();
    DcsInputServiceNewI dcsInputServiceNew = xformConfig.getDcsInputServiceNew();
    EntityIdentificationConfig entityIdentificationConfig = xformConfig.getEntityIdentificationConfig();

    Logger log = LoggerFactory.getLogger("dcs." + providerId + ".agent.groovy");
    // Fields required for callModifier
    def callModifierImplicitArgs = [:];
    callModifierImplicitArgs[PERFLOGMAPS] = perfLogMaps
    callModifierImplicitArgs[LOGGER] = log

    String fn = "userguid.xform";
    Map<String, Object> json = new HashMap<>();
    def parents = null;
    def ival = null;
    def xformBooleanCondition = false;

    // Precompute values
    def p0 = null;
    if (!(pe instanceof Map) && !(pe instanceof groovy.util.slurpersupport.GPathResult)) {
      log.error("Failed to parse {userGUID} from root node which was of type {" + pe.getClass() + "} instead of type Map");
    } else {
      p0 = pe["userGUID"];
    }
    def p1 = null;
    if (!(pe instanceof Map) && !(pe instanceof groovy.util.slurpersupport.GPathResult)) {
      log.error("Failed to parse {client_id} from root node which was of type {" + pe.getClass() + "} instead of type Map");
    } else {
      p1 = pe["client_id"];
    }

    // Assignments
    ival = p0;

    parents =  null; 
    assign(providerId, json, parents, "user_guid", ival, 
      fn, 1, "userGUID", "userGUID");

    ival = p1;

    parents =  null; 
    assign(providerId, json, parents, "client_id", ival, 
      fn, 2, "client_id", "client_id");



    if(null != dynamicMappingConfig  && dynamicMappingConfig.get("mappingType") != null &&  dcsInputServiceNew.getCustomKeyMappings()!=null && dcsInputServiceNew.getCustomKeyMappings().size() >0 ){
    if(pe != null){
          pe.each { key, val ->
         json = InjectedCode.callXform_dynamic(log,fn,pe,xformConfig,perfLogMaps,key,json,dynamicMappingConfig);
      }
      }
    }

    return json;
}