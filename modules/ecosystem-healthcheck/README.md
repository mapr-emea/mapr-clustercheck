TODO

# MapR Ecosystem Components HealthCheck

## PASSWORD AUTH

* Global parameters:
    - username & password
        + Used in most PAM/Plain authentication, credential file will be used and will be purged after each test, so that password will not be exposed in bash history
    - MapR user ticketfile
        + The default value is /opt/mapr/conf/mapruserticket
    - SSL certificate file
        + The default value is /opt/mapr/conf/ssl_truststore.pem


## Components Verification

* Hive :
    - hive-beeline-pam-ssl
        + This check reqires ssl truststore (/opt/mapr/conf/ssl_truststore) and hive configuration (<https://mapr.com/docs/home/Hive/HiveServer2-ConnectWithBeelineOrJDBC.html>).


## Output

* Output doesn't only provide the check results but also provide the check query performed or check steps for some complex check.