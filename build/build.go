package build

var (
	version   = "?"
	gitrev    = "?"
	builddate = "?"
)

func Version() string {
	return version
}

func GitRevision() string {
	return gitrev
}

func BuildDate() string {
	return builddate
}
