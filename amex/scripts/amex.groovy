public GenericAcquireResponse acquire(
    GenericAcquireRequest acquireRequest,
    HttpClient httpClient
) {
    PerfThreadContext.startTimer("groovy_processing_time");
    PerfThreadContext.set("dcs_spec_version",  "connectivity-dsl-acquire:0.31.18");
    GenericAcquireResponse genericAcquireResponse = new GenericAcquireResponse();
    genericAcquireResponse.setHttpStatusCode(HttpStatus.SC_OK);

    String[][] unboundInputVariables = [
      ["request_entities", "customerinfo"], ["provider_config", "apiKey"], ["request_entities", "account"], ["provider_config", "apiKey"], ["provider_config", "apiKey"], ["entity_data_level", "account"], ["provider_config", "apiKey"], ["provider_config", "apiKey"], ["provider_config", "apiKey"], ["provider_config", "downloadReceipts"]
    ]
    
}
