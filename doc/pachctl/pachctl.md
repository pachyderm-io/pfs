## pachctl



### Synopsis


Access the Pachyderm API.

Environment variables:
  PACHD_ADDRESS=<host>:<port>, the pachd server to connect to (e.g. 127.0.0.1:30650).
  PACH_CONFIG=<path>, the path where pachctl will attempt to load your pach config.
  JAEGER_ENDPOINT=<host>:<port>, the Jaeger server to connect to, if PACH_TRACE is set
  PACH_TRACE={true,false}, If true, and JAEGER_ENDPOINT is set, attach a
    Jaeger trace to any outgoing RPCs


### Options

```
  -v, --verbose   Output verbose logs
```

