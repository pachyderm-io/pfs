# Manage Cluster Access

Pachyderm contexts enable you to store configuration parameters for
multiple Pachyderm clusters in a single configuration file saved at
`~/.pachyderm/config.json`. This file stores the information
about all Pachyderm clusters that you have deployed from your
machine locally or on a remote server.

For example, if you have a cluster that
is deployed locally in `minikube` and another one deployed on
Amazon EKS, configurations for these clusters are stored in
that `config.json` file. By default, all local cluster configurations
have the `local` prefix. If you have multiple local clusters,
Pachyderm adds a consecutive number to the `local` prefix
of each cluster.

The following text is an example of a Pachyderm `config.json` file:

```bash
{
   "user_id": "b4fe4317-be21-4836-824f-6661c68b8fba",
   "v2": {
     "active_context": "local-1",
     "contexts": {
       "default": {},
       "local": {},
       "local-1": {},
     },
     "metrics": true
   }
}
```

## View the Active Context

When you have multiple Pachyderm clusters, you can switch
between them by setting the current context.
The active context is the cluster that you interact with when
you run `pachctl` commands.

To view active context, type:

* View the active context:

  ```bash
  $ pachctl config get active-context
  local-1
  ```

* List all contexts and view the current context:

  ```bash
  $ pachctl config list context
    ACTIVE  NAME
            default
            local
    *       local-1
  ```

  The active context is marked with an asterisk.

## Change the Active Context

To change the active context, type `pachctl config set
active-context <name>.`

## Create a New Context

You can create a new context by providing your parameters
through the standard input stream (`stdin`) in your terminal.
Specify the parameters as a comma-separated list enclosed in
curly brackets.

To create a new context with specific parameters, complete
the following steps:

1. Create a new Pachyderm context with a specific `pachd` IP address
and a client certificate:

   ```bash
   $ echo '{"pachd_address":"10.10.10.130:650", "server_cas":"key.pem"}' | pachctl config set context new-local
   Reading from stdin
   ```

   **Note:** By default, the `pachd` port is `30650` or `650`.

1. Verify your configuration by running the following command:

   ```bash
   $ pachctl config get context new-local
   {
     "pachd_address": "10.10.10.130:650",
     "server_cas": "key.pem"
   }
   ```

## Update an Existing Context

You can update an existing context with new parameters, such
as a Pachyderm IP address, certificate authority (CA), and others.
For the list of parameters, see [Pachyderm Config Specification](../reference/config_spec.html).

To update the Active Context, run the following commands:

1. Update the context with a new `pachd` address:

   ```bash
   $ pachctl config update context local-1 --pachd-address 10.10.10.131
   ```

   **Note:** The `pachctl config update` command supports the `--pachd-address`
   flag only.

1. Verify that the context has been updated:

   ```bash
   $ pachctl config get context local-1
   {
     "pachd_address": "10.10.10.131"
   }
   ```

1. Alternatively, you can update multiple properties by using
an `echo` script:

   **Example:**

   ```bash
   $ echo '{"pachd_address":"10.10.10.132", "server_cas":"key.pem"}' | pachctl config set context local-1 --overwrite
   Reading from stdin.
   ```

1. Verify that the changes were applied:

   ```bash
   $ pachctl config get context local-1
   {
     "pachd_address": "10.10.10.132",
     "server_cas": "key.pem"
   }
   ```
