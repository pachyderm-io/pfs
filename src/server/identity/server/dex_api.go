package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	dex_api "github.com/dexidp/dex/api/v2"
	dex_server "github.com/dexidp/dex/server"
	dex_storage "github.com/dexidp/dex/storage"
	"github.com/pkg/errors"
	logrus "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/client/identity"
)

// dexAPI wraps an api.DexServer and extends it with CRUD operations
// that are currently missing from the dex api. It also handles lazily
// instantiating the api and storage on the first request.
type dexAPI struct {
	sync.RWMutex

	server          dex_api.DexServer
	logger          *logrus.Entry
	storageProvider StorageProvider
}

func newDexAPI(sp StorageProvider, logger *logrus.Entry) *dexAPI {
	return &dexAPI{
		storageProvider: sp,
		logger:          logger,
	}
}

func (a *dexAPI) api() (dex_api.DexServer, error) {
	a.RLock()
	if a.server == nil {
		a.RUnlock()
		a.Lock()
		if a.server != nil {
			server := a.server
			a.Unlock()
			return server, nil
		}

		storage, err := a.storageProvider.GetStorage(a.logger)
		if err != nil {
			a.Unlock()
			return nil, err
		}
		a.server = dex_server.NewAPI(storage, nil)
		server := a.server
		a.Unlock()
		return server, nil
	}
	server := a.server
	a.RUnlock()

	return server, nil
}

func (a *dexAPI) createClient(ctx context.Context, in *identity.CreateOIDCClientRequest) (*identity.OIDCClient, error) {
	api, err := a.api()
	if err != nil {
		return nil, err
	}

	if in.Client.Name == "" {
		return nil, errors.New("no client name specified")
	}

	if in.Client.Id == "" {
		return nil, errors.New("no client id specified")
	}

	req := &dex_api.CreateClientReq{
		Client: &dex_api.Client{
			Id:           in.Client.Id,
			Secret:       in.Client.Secret,
			Name:         in.Client.Name,
			RedirectUris: in.Client.RedirectUris,
			TrustedPeers: in.Client.TrustedPeers,
		},
	}

	resp, err := api.CreateClient(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.AlreadyExists {
		return nil, fmt.Errorf("OIDC client with id %q already exists", req.Client.Id)
	}

	return dexClientToPach(resp.Client), nil
}

func (a *dexAPI) updateClient(ctx context.Context, in *identity.UpdateOIDCClientRequest) error {
	api, err := a.api()
	if err != nil {
		return err
	}

	req := &dex_api.UpdateClientReq{
		Id:           in.Client.Id,
		Name:         in.Client.Name,
		RedirectUris: in.Client.RedirectUris,
		TrustedPeers: in.Client.TrustedPeers,
	}

	resp, err := api.UpdateClient(ctx, req)
	if err != nil {
		return err
	}

	if resp.NotFound {
		return fmt.Errorf("unable to find OIDC client with id %q", req.Id)
	}

	return nil
}

func (a *dexAPI) deleteClient(ctx context.Context, id string) error {
	api, err := a.api()
	if err != nil {
		return err
	}

	resp, err := api.DeleteClient(ctx, &dex_api.DeleteClientReq{Id: id})
	if err != nil {
		return err
	}

	if resp.NotFound {
		return fmt.Errorf("unable to find OIDC client with id %q", id)
	}

	return nil
}

func (a *dexAPI) createConnector(req *identity.CreateIDPConnectorRequest) error {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return err
	}

	if req.Connector.Id == "" {
		return errors.New("no id specified")
	}

	if req.Connector.Type == "" {
		return errors.New("no type specified")
	}

	if req.Connector.Name == "" {
		return errors.New("no name specified")
	}

	if err := a.validateConnector(req.Connector.Id, req.Connector.Type, []byte(req.Connector.JsonConfig)); err != nil {
		return err
	}

	conn := dex_storage.Connector{
		ID:              req.Connector.Id,
		Type:            req.Connector.Type,
		Name:            req.Connector.Name,
		ResourceVersion: strconv.Itoa(int(req.Connector.ConfigVersion)),
		Config:          []byte(req.Connector.JsonConfig),
	}

	if err := storage.CreateConnector(conn); err != nil {
		return err
	}

	return nil
}

func (a *dexAPI) getConnector(id string) (*identity.IDPConnector, error) {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return nil, err
	}

	conn, err := storage.GetConnector(id)
	if err != nil {
		return nil, err
	}

	return dexConnectorToPach(conn), nil
}

func (a *dexAPI) updateConnector(in *identity.UpdateIDPConnectorRequest) error {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return err
	}

	return storage.UpdateConnector(in.Connector.Id, func(c dex_storage.Connector) (dex_storage.Connector, error) {
		oldVersion, _ := strconv.Atoi(c.ResourceVersion)
		if oldVersion+1 != int(in.Connector.ConfigVersion) {
			return dex_storage.Connector{}, fmt.Errorf("new config version is %v, expected %v", in.Connector.ConfigVersion, oldVersion+1)
		}

		c.ResourceVersion = strconv.Itoa(int(in.Connector.ConfigVersion))

		if in.Connector.Name != "" {
			c.Name = in.Connector.Name
		}

		if in.Connector.JsonConfig != "" {
			c.Config = []byte(in.Connector.JsonConfig)
		}

		if in.Connector.Type != "" {
			c.Type = in.Connector.Type
		}

		if err := a.validateConnector(c.ID, c.Type, c.Config); err != nil {
			return dex_storage.Connector{}, err
		}

		return c, nil
	})
}

func (a *dexAPI) deleteConnector(id string) error {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return err
	}

	return storage.DeleteConnector(id)
}

func (a *dexAPI) listConnectors() ([]*identity.IDPConnector, error) {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return nil, err
	}

	dexConnectors, err := storage.ListConnectors()
	if err != nil {
		return nil, err
	}

	connectors := make([]*identity.IDPConnector, len(dexConnectors))
	for i, c := range dexConnectors {
		connectors[i] = dexConnectorToPach(c)
	}
	return connectors, nil
}

func (a *dexAPI) listClients() ([]*identity.OIDCClient, error) {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return nil, err
	}

	storageClients, err := storage.ListClients()
	if err != nil {
		return nil, err
	}

	clients := make([]*identity.OIDCClient, len(storageClients))
	for i, c := range storageClients {
		clients[i] = storageClientToPach(c)
	}
	return clients, nil
}

func (a *dexAPI) getClient(id string) (*identity.OIDCClient, error) {
	storage, err := a.storageProvider.GetStorage(a.logger)
	if err != nil {
		return nil, err
	}

	client, err := storage.GetClient(id)
	if err != nil {
		return nil, err
	}
	return storageClientToPach(client), nil
}

func (a *dexAPI) validateConnector(id, connType string, jsonConfig []byte) error {
	typeConf, ok := dex_server.ConnectorsConfig[connType]
	if !ok {
		return fmt.Errorf("unknown connector type %q", connType)
	}

	conf := typeConf()
	if err := json.Unmarshal(jsonConfig, conf); err != nil {
		return fmt.Errorf("unable to deserialize JSON: %w", err)
	}

	if _, err := conf.Open(id, a.logger); err != nil {
		return fmt.Errorf("unable to open connector: %w", err)
	}

	return nil
}

func storageClientToPach(c dex_storage.Client) *identity.OIDCClient {
	return &identity.OIDCClient{
		Id:           c.ID,
		Secret:       c.Secret,
		RedirectUris: c.RedirectURIs,
		TrustedPeers: c.TrustedPeers,
		Name:         c.Name,
	}
}

func dexConnectorToPach(c dex_storage.Connector) *identity.IDPConnector {
	// If the version isn't an int, set it to zero
	version, _ := strconv.Atoi(c.ResourceVersion)
	return &identity.IDPConnector{
		Id:            c.ID,
		Name:          c.Name,
		Type:          c.Type,
		ConfigVersion: int64(version),
		JsonConfig:    string(c.Config),
	}
}

func dexClientToPach(c *dex_api.Client) *identity.OIDCClient {
	return &identity.OIDCClient{
		Id:           c.Id,
		Secret:       c.Secret,
		RedirectUris: c.RedirectUris,
		TrustedPeers: c.TrustedPeers,
		Name:         c.Name,
	}
}
