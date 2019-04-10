# MapR Ecosystem Components HealthCheck

## PASSWORD AUTH
* Credential file used and will be purged after test, so that password will not be exposed in bash history
    - For REST API with PAM
    - For components with PAM pain authen : credential

## Components Verification

* drill-jdbc-jsonfile-plainauth

* drill-jdbc-file-json-maprsasl

* drill-jdbc-maprdb-json-plainauth

* drill-ui-insecure

* drill-ui-secure-pam

* maprdb-json-shell

* maprdb-binary-shell