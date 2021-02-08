package client

import "github.com/pachyderm/pachyderm/v2/src/pfs"

type AppendFileOption func(*pfs.AppendFile)

func WithOverwrite() AppendFileOption {
	return func(af *pfs.AppendFile) {
		af.Overwrite = true
	}
}

func WithTag(tag string) AppendFileOption {
	return func(af *pfs.AppendFile) {
		af.Tag = tag
	}
}

func WithSplit(delimiter pfs.Delimiter, targetFileDatums, targetFileBytes int64) AppendFileOption {
	return func(af *pfs.AppendFile) {
		af.Delimiter = delimiter
		af.TargetFileDatums = targetFileDatums
		af.TargetFileBytes = targetFileBytes
	}

}
