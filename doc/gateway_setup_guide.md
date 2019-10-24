# Arweave Gateway Setup Guide

### Certificate files

Assuming the gateway will run under the domain name `gateway.example`, you will need to acquire a  certificate valid for both `gateway.example` and the wildcard `*.gateway.example`. This certificate's files should be installed at the following location:

- `apps/arweave/priv/tls/cert.pem` for the certificate file
- `apps/arweave/priv/tls/key.pem` for this certificate's key file

In order to allow the gateway to serve transactions under custom domain names, additional files need to be installed. For example, for a given domain name `custom.domain.example`, a certificate for that domain should be acquired and its files installed at the following location:

- `apps/arweave/priv/tls/custom.domain.example/cert.pem` for the certificate file
- `apps/arweave/priv/tls/custom.domain.example/key.pem` for this certificate's key file

### Custom domain DNS records

In order to point a custom domain name to a specific transaction a special DNS record needs to be created in its DNS zone.

For example, for a give custom domain name `custom.domain.example` and a given target transaction ID `1H0jHTlM6bYFdnrwZ4yMx92EgJITDRakse2YP_sDkBc`, a TXT record should be created with the name `_arweave.custom.domain.example` and the transaction ID as its value (`1H0jHTlM6bYFdnrwZ4yMx92EgJITDRakse2YP_sDkBc`).

### Startup

To run a node in gateway node, use the `gateway` command line flag or the `"gateway"` configuration field and specify which domain name should be this gateway's main domain name.

For example, with `gateway.example` as the gateway's main domain name:

Command line flag:
```
./arweave-server gateway gateway.example
```

Configuration field:
```jsonc
{
  // ...
  "gateway": "gateway.example"
}
```

To allow a transactions to be served from custom domains, use the command line flag `custom_domain` or the `"custom_domains"` configuration field and specify the custom domain name to serve from **in addition** to the `gateway` flag.

For example, given the custom domain names `custom1.domain.example` and `custom2.domain.example`:

Command line flag:
```
./arweave-server gateway gateway.example custom_domain custom1.domain.example custom_domain custom2.domain.example
```

Configuration field:
```jsonc
{
  // ...
  "gateway": "gateway.example"
  "custom_domains": [
    "custom1.domain.example",
    "custom2.domain.example"
  ]
}
```
