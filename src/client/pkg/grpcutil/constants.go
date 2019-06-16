package grpcutil

const (
	// TLSVolumePath is the path at which the tls cert and private key (if any)
	// will be mounted in the pachd pod
	TLSVolumePath = "/pachd-tls-cert"

	// TLSCertFile is the name of the mounted file containing a TLS certificate
	// that identifies pachd
	TLSCertFile = "tls.crt"

	// TLSKeyFile is the name of the mounted file containing a private key
	// corresponding to the public certificate in TLSCertFile
	TLSKeyFile = "tls.key"

	// MaxMsgSize is used to define the GRPC frame size
	MaxMsgSize = 20 * 1024 * 1024
)
