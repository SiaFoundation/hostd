package build

var (
	version   = "?"
	gitrev    = "?"
	builddate = "?"
)

// Version returns the version of the hostd binary.
func Version() string {
	return version
}

// GitRevision returns the git revision of the hostd binary.
func GitRevision() string {
	return gitrev
}

// Date returns the build date of the hostd binary.
func Date() string {
	return builddate
}
