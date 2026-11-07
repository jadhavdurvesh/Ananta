use ananta_api::{
    AqlErrorCode, AqlExecutionOptionsV1, CancellationToken, DbError, DbErrorCode,
    ExecutionConfig, ExecutionOptionsV1, AQL_EXECUTION_OPTIONS_V1_VERSION,
    API_VERSION, EXECUTION_OPTIONS_V1_VERSION,
};

#[test]
fn public_facade_versions_and_options_defaults_are_stable() {
    assert_eq!(API_VERSION, "0.1");
    assert_eq!(EXECUTION_OPTIONS_V1_VERSION, 1);
    assert_eq!(AQL_EXECUTION_OPTIONS_V1_VERSION, 1);

    let options = AqlExecutionOptionsV1::default();
    assert_eq!(options.version, AQL_EXECUTION_OPTIONS_V1_VERSION);
    assert_eq!(options.execution.version, EXECUTION_OPTIONS_V1_VERSION);
    assert_eq!(options.execution.limits, ExecutionConfig::default());
    assert!(options.validate().is_ok());
}

#[test]
fn public_facade_exposes_stable_error_classifications_and_cancellation() {
    let error = DbError::TableNotFound("telemetry".into());
    assert_eq!(error.code(), DbErrorCode::TableNotFound);
    assert_eq!(error.code().as_str(), "table_not_found");
    assert_eq!(AqlErrorCode::InvalidOptions.as_str(), "invalid_options");

    let token = CancellationToken::new();
    let clone = token.clone();
    clone.cancel();
    assert!(token.is_cancelled());

    let execution = ExecutionOptionsV1::new(ExecutionConfig::default());
    assert!(execution.validate().is_ok());
}
