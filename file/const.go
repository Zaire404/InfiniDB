package file

var (
	MagicText        = [4]byte{'I', 'D', 'B', 'M'}
	MagicVersion     = 1
	ManifestName     = "MANIFEST"
	ReManifestName   = "REMANIFEST"
	RewriteThreshold = 1000
	RewriteRatio     = 10
)
