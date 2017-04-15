package version

import (
	"fmt"

	pb "github.com/pachyderm/pachyderm/src/client/version/versionpb"
)

const (
	// MajorVersion is the current major version for pachyderm.
	MajorVersion = 1
	// MinorVersion is the current minor version for pachyderm.
	MinorVersion = 4
	// MicroVersion is the patch number for pachyderm.
	MicroVersion = 3
)

var (
	// AdditionalVersion is the string provided at release time
	// The value is passed to the linker at build time
	// DO NOT set the value of this variable here
	AdditionalVersion string
	// Version is the current version for pachyderm.
	Version = &pb.Version{
		Major:      MajorVersion,
		Minor:      MinorVersion,
		Micro:      MicroVersion,
		Additional: AdditionalVersion,
	}
)

// PrettyPrintVersion returns a version string optionally tagged with metadata.
// For example: "1.2.3", or "1.2.3-rc1" if version.Additional is "rc1".
func PrettyPrintVersion(version *pb.Version) string {
	result := fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Micro)
	if version.Additional != "" {
		result += fmt.Sprintf("-%s", version.Additional)
	}
	return result
}
