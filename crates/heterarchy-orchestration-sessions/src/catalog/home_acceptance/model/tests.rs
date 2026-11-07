use super::*;

#[test]
fn operations_source_reference_must_equal_the_actual_source_payload_digest() {
    let source = [0x41; DIGEST_BYTES];
    let operations = [0x42; DIGEST_BYTES];
    assert!(validate_source_digest(&source, &source).is_ok());
    assert!(matches!(
        validate_source_digest(&source, &operations),
        Err(Error::InvalidResolution(
            "Operations Source terminal digest"
        ))
    ));
    assert!(matches!(
        validate_source_digest(&[0; DIGEST_BYTES], &[0; DIGEST_BYTES]),
        Err(Error::InvalidResolution(
            "Operations Source terminal digest"
        ))
    ));
}
